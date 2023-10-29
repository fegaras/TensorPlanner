/*
 * Copyright © 2023 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uta.diablo

import AST._
import Typechecker._
import scala.collection.mutable.ArrayBuffer
import java.io.Serializable
import java.util.Calendar


// Converts a diablo AST to an asynchronous plan
object PlanGenerator {
  var trace = false

  type OprID = Int
  type WorkerID = Int
  type FunctionID = Int
  type BlockID = Int

  def info ( s: String ) {
    if (trace) {
      val now = Calendar.getInstance()
      val hh = now.get(Calendar.HOUR)
      val mm = now.get(Calendar.MINUTE)
      val ss = now.get(Calendar.SECOND)
      val ms = now.get(Calendar.MILLISECOND)
      printf("[%02d:%02d:%02d:%03d]   %s\n",hh,mm,ss,ms,s)
    }
  }

  type Status = Short
  val List(notReady,scheduled,ready,computed,completed,removed): List[Status] = List(0,1,2,3,4,5)

  // Operation tree (pilot plan)
  @SerialVersionUID(123L)
  sealed abstract
  class Opr ( var node: WorkerID = -1,        // worker node
              var size: Int = -1,             // num of blocks in output
              var static_blevel: Int = -1,    // static b-level (bottom level)
              var status: Status = notReady,  // the operation status
              var visited: Boolean = false,   // used in BFS traversal
              @transient
              var cached: Any = null,         // cached result blocks
              var consumers: List[OprID] = Nil,
              var count: Int = 0,             // = number of local consumers
              var reduced_count: Int = 0,     // # of reduced inputs so far
              var os: List[OprID] = Nil,      // closest descendant producers on the same node
              var oc: Int = 0 )               // counts the closest ancestor consumers on the same node
        extends Serializable
  case class LoadOpr ( block: BlockID ) extends Opr
  case class PairOpr ( x: OprID, y: OprID ) extends Opr
  case class ApplyOpr ( x: OprID, fnc: FunctionID ) extends Opr
  case class ReduceOpr ( s: List[OprID], valuep: Boolean, op: FunctionID ) extends Opr
  case class SeqOpr ( s: List[OprID] ) extends Opr

  // Opr uses OprID for Opr references
  var operations: ArrayBuffer[Opr] = ArrayBuffer[Opr]()
  // functions used by ApplyOpr and ReduceOpr
  val functions: ArrayBuffer[Expr] = ArrayBuffer[Expr]()
  // blocks used by LoadOpr
  val loadBlocks: ArrayBuffer[Any] = ArrayBuffer[Any]()

  def children ( e: Opr ): List[OprID]
    = e match {
        case PairOpr(x,y)
          => List(x,y)
        case ApplyOpr(x,_)
          => List(x)
        case SeqOpr(s)
          => s
        case ReduceOpr(s,_,_)
          => s
        case _ => Nil
      }

  def print_plan[I] ( e: Plan[I] ) {
    val exit_points = e._3.map(x => x._2)
    info("Exit points: "+exit_points)
    info("Operations: "+operations.length)
    for ( opr_id <- operations.indices) {
      val opr = operations(opr_id)
      info(""+opr_id+")  node="+opr.node+"  size="+opr.size
           +"  blevel="+opr.static_blevel
           +"   consumers="+opr.consumers+"   "+opr)
    }
  }

  def isRDD ( e: Expr ): Boolean
    = e match {
        case Call("diablo_join",_) => true
        case Call("diablo_cogroup",_) => true
        case flatMap(_,x) => isRDD(x)
        case MethodCall(x,"reduceByKey",_) => true
        case MethodCall(_,"parallelize",_) => true
        case Nth(Var(_),3) => true
        case _ => false
      }

  def makeType ( tp: Type ): Type = {
    def rep ( n: Int ): Type
      = TupleType(1.to(n).toList.map(_ => intType))
    tp match {
      case StorageType(f@btpat(_,_,dn,sn),tps,args)
        => TupleType(List(rep(dn.toInt),rep(sn.toInt),
                          SeqType(TupleType(List(rep(dn.toInt+sn.toInt),
                                                 intType)))))
      case _ => apply(tp,makeType)
    }
  }

  def getKey ( e: Expr ): Option[Expr] = {
    e match {
        case Seq(List(Tuple(List(key,ta))))
          => Some(key)
        case flatMap(Lambda(f,b),x)
          => for ( bc <- getKey(b) )
                yield flatMap(Lambda(f,bc),x)
        case IfE(p,x,y)
          => for ( xc <- getKey(x) )
                yield IfE(p,xc,y)
        case Let(p,x,b)
          => for ( bc <- getKey(b) )
                yield Let(p,x,bc)
        case _ => None
      }
  }

  def getApplyOprs ( p: Pattern, e: Expr ): Expr
    = (p,e) match {
         case (TuplePat(List(x,y)),flatMap(f,Call("diablo_join",a::b::_)))
           => Call("applyOpr",
                   List(Call("pairOpr",
                             List(getApplyOprs(x,a),
                                  getApplyOprs(y,b))),
                        function(f)))
         case (TuplePat(List(x,y)),flatMap(f,Call("diablo_cogroup",a::b::_)))
           => Call("applyOpr",
                   List(Call("pairOpr",
                             List(Call("seqOpr",List(getApplyOprs(x,a))),
                                  Call("seqOpr",List(getApplyOprs(y,b))))),
                        function(f)))
         case (TuplePat(List(index,VarPat(opr))),flatMap(f,_))
           => Call("applyOpr",List(Var(opr),function(f)))
         case _ => toExpr(p)
      }

  def function ( e: Expr ): Expr = {
    val i = functions.indexOf(e)
    IntConst(if (i >= 0)
               i
             else { functions += e; functions.length-1 })
  }

  // generates code that constructs a pilot plan of type List[(index,OprID)]
  def makePlan ( e: Expr, topJoin: Boolean ): Expr = {
    e match {
        case Nth(Var(v),3)
          if typecheck_var(v).isDefined
             && (typecheck(e) match {
                   case ParametricType(rdd,_) => rdd == rddClass
                   case _ => false })
          // Scala variable bound to an RDD tensor defined outside the macro
          => val i = newvar
             val v = newvar
             Comprehension(Tuple(List(Var(i),
                                      Call("loadOpr",
                                           List(Tuple(List(Var(i),Var(v))))))),
                           List(Generator(TuplePat(List(VarPat(i),VarPat(v))),
                                          e)))
        case Nth(Var(v),3)
          // bound to a tensor plan
          => e
        case Call("diablo_join",x::y::_)
          => Call("join",List(makePlan(x,false),makePlan(y,false)))
        case Call("diablo_cogroup",x::y::_)
          => val xp = makePlan(x,false)
             val yp = makePlan(y,false)
             Call("cogroup",List(xp,yp))
        case flatMap(Lambda(p,b),MethodCall(_,"parallelize",x::_))
          => val k = newvar
             val v = newvar
             MethodCall(Comprehension(Tuple(List(Var(k),
                                                 Call("loadOpr",
                                                      List(Tuple(List(Var(k),Var(v))))))),
                             List(Generator(p,x),
                                  Generator(TuplePat(List(VarPat(k),VarPat(v))),
                                            b))),
                        "toList",null)
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x@Call(join,_))
          if List("diablo_join","diablo_cogroup").contains(join)
             && getKey(b).isDefined
          => val xp = makePlan(x,false)
             val Some(key) = getKey(b)
             val gl = if (topJoin)
                        getApplyOprs(pp,e)
                      else toExpr(pp)
             Comprehension(Tuple(List(key,gl)),
                           List(Generator(p,xp)))
        case flatMap(f@Lambda(p@TuplePat(List(_,pp)),b),x)
          if topJoin && getKey(b).isDefined
          => val xp = makePlan(x,topJoin)
             val Some(key) = getKey(b)
             val gl = Call("applyOpr",List(toExpr(pp),function(f)))
             Comprehension(Tuple(List(key,gl)),
                           List(Generator(p,xp)))
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x)
          if getKey(b).isDefined
          => val xp = makePlan(x,topJoin)
             val Some(key) = getKey(b)
             val gl = toExpr(pp)
             Comprehension(Tuple(List(key,Tuple(List(toExpr(kk),gl)))),
                           List(Generator(p,xp)))
        case MethodCall(x,"reduceByKey",List(op,_))
          => val k = newvar
             val s = newvar
             val xp = makePlan(x,topJoin)
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(s))),
                            Seq(List(Tuple(List(Var(k),
                                                Call("reduceOpr",
                                                     List(Var(s),
                                                          BoolConst(false),function(op)))))))),
                     Call("groupByKey",List(xp)))
        case flatMap(f,x)
          => val k = newvar
             val gl = newvar
             val xp = makePlan(x,topJoin)
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(gl))),
                            Seq(List(Tuple(List(Var(k),
                                                Call("applyOpr",
                                                     List(Var(gl),function(f)))))))),
                     xp)
        case _ => apply(e,makePlanExpr)
      }
  }

  def makePlanExpr ( e: Expr ): Expr
    = if (isRDD(e))
        makePlan(e,true)
      else e match {
             case VarDecl(v,tp,x)
               => VarDecl(v,makeType(tp),makePlanExpr(x))
             case MethodCall(x,"reduce",op::_)
               if isRDD(x)
               => // a total aggregation must be evaluated during the planning stage (eager)
                  Coerce(Call("evalOpr",List(Call("reduceOpr",
                                                  List(flatMap(Lambda(VarPat("x"),
                                                                      Seq(List(Nth(Var("x"),2)))),
                                                               makePlan(x,true)),
                                                       BoolConst(true),
                                                       function(op))))),
                         elemType(typecheck(x)))
             case _ => apply(e,makePlanExpr)
           }
}

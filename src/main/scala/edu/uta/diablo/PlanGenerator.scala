/*
 * Copyright © 2024-2024 University of Texas at Arlington
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
  val List(notReady,scheduled,ready,computed,completed,removed,locked,zombie): List[Status] = List(0,1,2,3,4,5,6,7)

  // Operation tree (pilot plan)
  @SerialVersionUID(123L)
  sealed abstract
  class Opr ( var node: WorkerID = -1,        // worker node
              var coord: Any = (),            // block coordinates of the operator
              var size: Int = -1,             // num of blocks in output
              var static_blevel: Int = -1,    // static b-level (bottom level)
              var status: Status = notReady,  // the operation status
              var visited: Boolean = false,   // used in BFS traversal
              @transient
              var cached: Any = null,         // cached result blocks
              var encoded_type: List[Int] = Nil, // type encoding of the cached block (for C++)
              var consumers: List[OprID] = Nil,
              var count: Int = 0,             // = number of local consumers
              var reduced_count: Int = 0,     // # of reduced inputs so far
              var cpu_cost: Int = 0,          // number of nested loops when processing blocks
              var os: List[OprID] = Nil,      // closest descendant producers on the same node
              var oc: Int = 0 )               // counts the closest ancestor consumers on the same node
        extends Serializable

  case class LoadOpr ( block: BlockID ) extends Opr {
    override def hashCode (): Int = (1,block).##
    override def equals ( that: Any ): Boolean
      = that match {
          case that: LoadOpr => that.block == block
          case _ => false
        }
  }
  case class PairOpr ( x: OprID, y: OprID ) extends Opr {
    override def hashCode (): Int = (2,x,y).##
    override def equals ( that: Any ): Boolean
      = that match {
          case that: PairOpr => that.x == x && that.y == y
          case _ => false
        }
  }
  case class ApplyOpr ( x: OprID, fnc: FunctionID, extra_args: Any ) extends Opr {
    override def hashCode (): Int = (3,x,fnc,extra_args).##
    override def equals ( that: Any ): Boolean
      = that match {
          case that: ApplyOpr => that.x == x && that.fnc == fnc
          case _ => false
        }
  }
  case class ReduceOpr ( s: List[OprID], valuep: Boolean, op: FunctionID ) extends Opr {
    override def hashCode (): Int = (4,s,valuep,op).##
    override def equals ( that: Any ): Boolean
      = that match {
          case that: ReduceOpr
            => (that.valuep == valuep && that.op == op
                && that.s.length == s.length && that.s.toSet == s.toSet)
          case _ => false
        }
  }
  case class SeqOpr ( s: List[OprID] ) extends Opr {
    override def hashCode (): Int = (5,s).##
    override def equals ( that: Any ): Boolean
      = that match {
          case that: SeqOpr => that.s == s
          case _ => false
        }
  }

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
        case ApplyOpr(x,_,_)
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
           +"  cpu_cost="+opr.cpu_cost+"  coords="+opr.coord
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

  // encode the type to a list of Int (used in C++ run-time)
  def encodeType ( tp: Type ): Expr = {
    var v = ArrayBuffer[Int]()
    def et ( tp: Type ) {
      tp match {
        case TupleType(ts)
          => v += 10
             v += ts.length
             ts.foreach(et)
        case ArrayType(_,t)
          => v += 11
             et(t)
        case SeqType(t)
          => v += 12
             et(t)
        case BasicType(_)
          => if (tp == intType)
               v += 0
             else if (tp == longType)
                    v += 1
             else if (tp == boolType)
                    v += 2
             else if (tp == doubleType)
                    v += 3
             else if (tp == stringType)
                    v += 4
             else if (tp == emptyTupleType) {
                      v += 10
                      v += 0
                    }
             else v += -2
        case _ => v += -1
      }
    }
    et(tp)
    Seq(v.map(IntConst(_)).toList)
  }

  def function ( e: Expr ): Expr = {
    val i = functions.indexOf(e)
    IntConst(if (i >= 0)
               i
             else { functions += e; functions.length-1 })
  }

  def embedApplyOpr ( e: Expr, f: Lambda, tv: String, args: List[Pattern], idx: Option[Expr] ): Option[Expr]
    = e match {
        case Seq(List(el@Tuple(List(key,ta))))
          => val tp = typecheck(el)
             val t = Call("applyOpr",
                          List(Var(tv),
                               function(if (args.isEmpty)
                                          f
                                        else Lambda(TuplePat(args),f)),
                               Tuple(args.map(toExpr)),
                               key,
                               IntConst(cpu_cost(f)),
                               encodeType(tp)))
             Some(Seq(List(Tuple(List(key,if (idx.nonEmpty)
                                            Tuple(List(idx.head,t))
                                          else t)))))
        case flatMap(g@Lambda(p,b),x)
          => val Lambda(q,d) = f
             for ( bc <- embedApplyOpr(b,Lambda(q,b),tv,args:+p,idx) )
                yield flatMap(Lambda(p,bc),x)
        case IfE(p,x,y)
          => for ( xc <- embedApplyOpr(x,f,tv,args,idx) )
                yield IfE(p,xc,y)
        case Let(p,x,b)
          => for ( bc <- embedApplyOpr(b,f,tv,args,idx) )
                yield Let(p,x,bc)
        case _ => None
      }

  def getPlanIndex ( e: Expr ): Option[Expr]
    = e match {
        case Seq(List(Tuple(List(_,Tuple(List(key,_))))))
          => Some(key)
        case IfE(p,x,y)
          => getPlanIndex(x)
        case Let(p,x,b)
          => getPlanIndex(b)
        case _ => None
      }

  def cpu_cost ( e: Expr ): Int
    = e match {
        case Call("merge_tensors",_)
          => 1
        case flatMap(x,_)
          => 1+cpu_cost(x)
        case _ => AST.accumulate[Int](e,cpu_cost,Math.max(_,_),0)
      }

  def getIndices ( p: Pattern ): Pattern
    = p match {
        case TuplePat(List(i,VarPat(_)))
          => i
        case TuplePat(List(p1,p2))
          => TuplePat(List(getIndices(p1),getIndices(p2)))
        case _ => p
      }

  // generates code that constructs a pilot plan of type List[(index,OprID)]
  def makePlan ( e: Expr, top: Boolean ): Expr = {
    def getJoinType ( join: String ): Option[String]
      = join match {
          case "diablo_join" => Some("join")
          case "diablo_cogroup" => Some("cogroup")
          case "join" => Some("join")
          case "cogroup" => Some("cogroup")
          case _ => None
        }
    def isJoinOrRBK ( x: Expr ): Boolean
      = x match {
          case Call(join,_) => getJoinType(join).nonEmpty
          case MethodCall(_,"reduceByKey",_) => true
          case _ => false
        }
    e match {
        case Nth(Var(v),3)
          if typecheck_var(v).isDefined
             && (typecheck(e) match {
                   case ParametricType(rdd,_) => rdd == rddClass
                   case _ => false })
          // Scala variable bound to an RDD tensor defined outside the macro
          => val i = newvar
             val v = newvar
             val tp = elemType(typecheck(e))
             Comprehension(Tuple(List(Var(i),
                                      Call("loadOpr",
                                           List(Tuple(List(Var(i),Var(v))),
                                                Var(i),
                                                encodeType(tp))))),
                           List(Generator(TuplePat(List(VarPat(i),VarPat(v))),
                                          e)))
        case Nth(Var(v),3)
          // bound to a tensor plan
          => e
        case Call(join,x::y::_)
          if getJoinType(join).nonEmpty
          => val xp = makePlan(x,false)
             val yp = makePlan(y,false)
             val Some(inMemJoin) = getJoinType(join)
             val k = newvar; val ix = newvar; val tx = newvar
             val iy = newvar; val ty = newvar
             val itp = TuplePat(List(TuplePat(List(VarPat(ix),VarPat(tx))),
                                     TuplePat(List(VarPat(iy),VarPat(ty)))))
             val tp = elemType(typecheck(e))
             val pair = Call("pairOpr",
                             if (inMemJoin == "join")
                               List(Var(tx),Var(ty),Var(k),encodeType(tp))
                             else List(Call("seqOpr",List(Var(tx))),
                                       Call("seqOpr",List(Var(ty))),
                                       Var(k),encodeType(tp)))
             Comprehension(Tuple(List(Var(k),
                                      Tuple(List(Tuple(List(Var(ix),Var(iy))),
                                                 pair)))),
                           List(Generator(TuplePat(List(VarPat(k),itp)),
                                          Call(inMemJoin,List(xp,yp)))))
        case flatMap(Lambda(p,b),MethodCall(_,"parallelize",x::_))
          => val k = newvar
             val v = newvar
             val tp = elemType(typecheck(e))
             MethodCall(Comprehension(Tuple(List(Var(k),
                                                 Call("loadOpr",
                                                      List(Tuple(List(Var(k),Var(v))),
                                                           Var(k),
                                                           encodeType(tp))))),
                             List(Generator(p,x),
                                  Generator(TuplePat(List(VarPat(k),VarPat(v))),
                                            b))),
                        "toList",null)
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x@Call(join,_))
          if embedApplyOpr(b,f,"",Nil,None).nonEmpty && getJoinType(join).nonEmpty
          => val xp = makePlan(x,false)
             val jk = newvar; val iv = newvar; val tv = newvar
             val ip = getIndices(pp)
             val Some(key) = embedApplyOpr(b,f,tv,Nil,
                                   if (top) None
                                   else getPlanIndex(b).orElse(Some(toExpr(getIndices(pp)))))
             flatMap(Lambda(TuplePat(List(VarPat(jk),TuplePat(List(ip,VarPat(tv))))),
                            key),
                     xp)
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x@Call(join,_))
          if false && embedApplyOpr(b,f,"",Nil,None).nonEmpty && getJoinType(join).nonEmpty
          => val xp = makePlan(x,false)
             val jk = newvar; val iv = newvar; val tv = newvar
             val ip = getIndices(pp)
             val Some(key) = embedApplyOpr(b,f,tv,Nil,
                                   if (top) None else Some(toExpr(ip)))
             flatMap(Lambda(TuplePat(List(VarPat(jk),TuplePat(List(ip,VarPat(tv))))),
                            key),
                     xp)
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x@Call(join,_))
          if false && embedApplyOpr(b,f,"",Nil,None).nonEmpty && getJoinType(join).nonEmpty
          => val xp = makePlan(x,false)
             val jk = newvar; val iv = newvar; val tv = newvar
             val ip = getIndices(pp)
             val Some(key) = embedApplyOpr(b,f,tv,Nil,
                                   if (top) None
                                   else getPlanIndex(b).orElse(Some(toExpr(getIndices(pp)))))
             flatMap(Lambda(TuplePat(List(VarPat(jk),TuplePat(List(ip,VarPat(tv))))),
                            key),
                     xp)
        case flatMap(f@Lambda(p@TuplePat(List(kk,pp)),b),x@Call(join,_))
          if false && embedApplyOpr(b,f,"",Nil,None).nonEmpty && getJoinType(join).nonEmpty
          => val xp = makePlan(x,false)
             val jk = newvar; val iv = newvar; val tv = newvar
             val ip = getIndices(pp)
             val Some(key) = embedApplyOpr(b,f,tv,Nil,
                                   if (top) None else Some(toExpr(ip)))
             flatMap(Lambda(TuplePat(List(VarPat(jk),TuplePat(List(ip,VarPat(tv))))),
                            key),
                     xp)
        case flatMap(f@Lambda(p@TuplePat(List(ip,pp)),b),x)
          if embedApplyOpr(b,f,"",Nil,None).nonEmpty
          => val tv = newvar; val k = newvar
             val xp = makePlan(x,top)
             val Some(key) = embedApplyOpr(b,f,tv,Nil,
                                   if (top) None else Some(toExpr(ip)))
             flatMap(Lambda(TuplePat(List(ip,VarPat(tv))),
                            key),
                     xp)
        case flatMap(f,x)
          => val k = newvar
             val gl = newvar
             val xp = makePlan(x,top)
             val tp = elemType(typecheck(e))
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(gl))),
                            Seq(List(Tuple(List(Var(k),
                                                Call("applyOpr",
                                                     List(Var(gl),
                                                          function(f),
                                                          Tuple(Nil),
                                                          Var(k),
                                                          IntConst(cpu_cost(f)),
                                                          encodeType(tp)))))))),
                     xp)
        case MethodCall(x,"reduceByKey",List(op,_))
          => val k = newvar
             val s = newvar
             val xp = makePlan(x,false)
             val tp = elemType(typecheck(e))
             val rv = Call("reduceOpr",
                           List(flatMap(Lambda(VarPat("x"),Seq(List(Nth(Var("x"),2)))),
                                        Var(s)),
                                BoolConst(false),
                                function(op),
                                Var(k),
                                IntConst(cpu_cost(op)),
                                encodeType(tp)))
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(s))),
                            Seq(List(Tuple(List(Var(k),rv))))),
                     Call("groupByKey",List(xp)))
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
                  val tp = typecheck(e)
                  Coerce(Call("evalOpr",List(Call("reduceOpr",
                                                  List(flatMap(Lambda(VarPat("x"),
                                                                      Seq(List(Nth(Var("x"),2)))),
                                                               makePlan(x,true)),
                                                       BoolConst(true),
                                                       function(op),
                                                       Tuple(Nil),
                                                       IntConst(cpu_cost(op)),
                                                       encodeType(tp))))),
                         tp)
             case _ => apply(e,makePlanExpr)
           }
}

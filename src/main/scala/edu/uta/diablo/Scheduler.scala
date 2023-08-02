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
import scala.collection.mutable.{ArrayBuffer,ListBuffer}
import java.io.Serializable

object Scheduler {
  val trace = false

  type OprID = Int
  type WorkerID = Int
  type FunctionID = Int

  // Operation tree (pilot plan)
  sealed abstract class Opr ( var node: WorkerID = -1,     // worker node
                              var size: Int = 0,           // num of blocks in output
                              var static_blevel: Int = -1, // static b-level (bottom level)
                              @transient
                              var cached: Any = null,      // cached result block(s)
                              var consumers: List[OprID] = Nil,
                              var count: Int = 0 )         // number of consumers
                  extends Serializable
  case class LoadOpr ( index: Any, block: () => Any ) extends Opr
  case class TupleOpr ( x: OprID, y: OprID ) extends Opr
  case class ApplyOpr ( x: OprID, fnc: FunctionID ) extends Opr
  case class ReduceOpr ( s: List[OprID], op: FunctionID ) extends Opr

  // Opr uses OprID for Opr an reference
  var operations: ArrayBuffer[Opr] = ArrayBuffer[Opr]()
  // functions used by ApplyOpr and ReduceOpr
  val functions: ArrayBuffer[Expr] = ArrayBuffer[Expr]()

  def children ( e: Opr ): List[OprID]
    = e match {
        case TupleOpr(x,y)
          => List(x,y)
        case ApplyOpr(x,_)
          => List(x)
        case ReduceOpr(s,_)
          => s
        case _ => Nil
      }

  def print_plan[I,T,S] ( e: Plan[I,T,S] ) {
    def pp ( opr_id: OprID, tabs: Int ) {
      val x = operations(opr_id)
      println(" "*tabs*3+tabs+")  node="+x.node+"  size="+x.size+"  blevel="
              +x.static_blevel+"   consumers="+x.consumers+"  "+opr_id+" "+x)
      children(x).foreach(c => pp(c,tabs+1))
    }
    e._3.foreach(x => pp(x._2._3,1))
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
                                                 TupleType(List(rep(dn.toInt),rep(sn.toInt),
                                                                intType)))))))
      case _ => apply(tp,makeType)
    }
  }

  def findKey ( e: Expr ): Option[Expr] = {
    def findDims ( e: Expr ): Expr
      = e match {
          case Tuple(List(dp,sp,_))
            => Tuple(List(dp,sp))
          case IfE(p,x,y)
            => IfE(p,findDims(x),y)
          case Let(p,x,b)
            => Let(p,x,findDims(b))
          case _ => e
        }
    e match {
        case Seq(List(Tuple(List(key,ta))))
          => val ds = findDims(ta)
             Some(Seq(List(Tuple(List(key,ds)))))
        case flatMap(Lambda(f,b),x)
          => for ( bc <- findKey(b) )
                yield flatMap(Lambda(f,bc),x)
        case IfE(p,x,y)
          => for ( xc <- findKey(x) )
                yield IfE(p,xc,y)
        case Let(p,x,b)
          => for ( bc <- findKey(b) )
                yield Let(p,x,bc)
        case _ => None
      }
  }

  def getOpr ( e: Pattern ): Option[Expr] = {
    def gl ( e: Pattern ): Option[Expr]
      = e match {
          case TuplePat(List(key,VarPat(v)))
            => Some(Nth(Var(v),3))
          case TuplePat(List(x,y))
            => for { xl <- gl(x)
                     yl <- gl(y) }
                  yield Call("tupleOpr",List(xl,yl))
          case _ => None
        }
    e match {
      case TuplePat(List(_,VarPat(v)))
        => Some(Nth(Var(v),3))
      case TuplePat(List(k,x:TuplePat))
        => gl(x)
      case _ => None
    }
  }

  def function ( e: Expr ): Expr = {
    val i = functions.indexOf(e)
    IntConst(if (i >= 0)
               i
             else { functions += e; functions.length-1 })
  }

  // generates code that constructs a pilot plan of type (index,(dense-dims,sparse-dims,OprID))
  def makePlan ( e: Expr ): Expr = {
    def keySize ( x: Expr ): Int
      = elemType(typecheck(x)) match {
          case TupleType(List(BasicType(_),_)) => 1
          case TupleType(List(TupleType(ts),_)) => ts.length
        }
      e match {
        case Nth(Var(v),3)
          if typecheck_var(v).isDefined
          // Scala variable bound to an RDD tensor defined outside the macro
          => val i = newvar
             val gl = newvar
             val dp = newvar
             val sp = newvar
             Comprehension(Tuple(List(Var(i),
                              Tuple(List(Var(dp),Var(sp),
                                      Call("loadOpr",
                                           List(Var(i),
                                                function(Lambda(TuplePat(Nil),
                                                      Tuple(List(Var(i),
                                                                 Tuple(List(Var(dp),Var(sp),
                                                                            Var(gl))))))))))))),
                           List(Generator(TuplePat(List(VarPat(i),
                                              TuplePat(List(VarPat(dp),VarPat(sp),VarPat(gl))))),
                                          e)))
        case Nth(Var(v),3)
          // bound to a tensor plan
          => e
        case Call(join,x::y::_)
          if List("diablo_join","diablo_cogroup").contains(join)
          => val n = keySize(x)
             val xs = 1.to(n).map(i => newvar).toList
             val ys = 1.to(n).map(i => newvar).toList
             val xvp = tuple(xs.map(VarPat))
             val yvp = tuple(ys.map(VarPat))
             val xl = newvar
             val yl = newvar
             val xp = makePlan(x)
             val yp = makePlan(y)
             val preds = (xs zip ys).map { case (i,j) =>
                                Predicate(MethodCall(Var(i),"==",List(Var(j)))) }
             Comprehension(Tuple(List(toExpr(xvp),
                                      Tuple(List(Var(xl),Var(yl))))),
                           Generator(TuplePat(List(xvp,VarPat(xl))),xp)
                           ::Generator(TuplePat(List(yvp,VarPat(yl))),yp)
                           ::preds)
        case flatMap(f@Lambda(p,b),x)
          if getOpr(p).isDefined && findKey(b).isDefined
          => val k = newvar
             val xl = newvar
             val dp = newvar
             val sp = newvar
             val xp = makePlan(x)
             val Some(gl) = getOpr(p)
             val Some(key) = findKey(b)
             Comprehension(Tuple(List(Var(k),
                                      Tuple(List(Var(dp),Var(sp),
                                                 Call("applyOpr",
                                                      List(gl,function(f))))))),
                           List(Generator(p,xp),
                                Generator(TuplePat(List(VarPat(k),
                                               TuplePat(List(VarPat(dp),VarPat(sp))))),
                                          key)))
        case flatMap(Lambda(p,b),MethodCall(_,"parallelize",x::_))
          => val k = newvar
             val i = newvar
             val xl = newvar
             val dp = newvar
             val sp = newvar
             MethodCall(Comprehension(Tuple(List(Var(k),
                                             Tuple(List(Var(dp),Var(sp),
                                                        Call("loadOpr",
                                                             List(Var(k),
                                                                  Lambda(TuplePat(Nil),
                                                                         Tuple(List(Var(k),
                                                                                Tuple(List(Var(dp),Var(sp),
                                                                                      Var(xl)))))))))))),
                             List(Generator(p,x),
                                  Generator(TuplePat(List(VarPat(k),
                                                  TuplePat(List(VarPat(dp),VarPat(sp),
                                                                VarPat(xl))))),
                                            b))),
                        "toList",null)
        case MethodCall(x,"reduceByKey",List(op,_))
          => val xl = newvar
             val xdp = newvar
             val xsp = newvar
             val k = newvar
             val s = newvar
             val nv = newvar
             val xp = makePlan(x)
             Let(VarPat(nv),xp,
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(s))),
                  Let(TuplePat(List(VarPat(xdp),VarPat(xsp),VarPat(xl))),
                      Nth(MethodCall(Var(nv),"head",null),2),
                      Seq(List(Tuple(List(Var(k),Tuple(List(Var(xdp),Var(xsp),
                                     Call("reduceOpr",
                                        List(flatMap(Lambda(TuplePat(List(VarPat(xdp),VarPat(xsp),
                                                                          VarPat(xl))),
                                                            Seq(List(Var(xl)))),
                                                     Var(s)),
                                             function(op))))))))))),
                 Call("groupByKey",List(Var(nv)))))
        case flatMap(_,_)
          => throw new Error("Unrecognized flatMap: "+e)
        case _ => apply(e,makePlanExpr)
      }
  }

  def makePlanExpr ( e: Expr ): Expr
    = if (isRDD(e))
        makePlan(e)
      else e match {
             case VarDecl(v,tp,x)
               => VarDecl(v,makeType(tp),makePlanExpr(x))
             case _ => apply(e,makePlanExpr)
           }

/****************************************************************************************************/

  var max_lineage_length = 3

  // work done at each processor
  var work: Array[Int] = _
  // # of tasks at each processor
  var tasks: Array[Int] = _
  // the pool of ready nodes
  var ready_pool: ListBuffer[OprID] = _

  def size ( e: Opr ): Int = {
    children(e).map(x => size(operations(x)))
    e.size = e match {
               case TupleOpr(x,y)
                 => operations(x).size + operations(y).size
               case _ => 1
             }
    e.size
  }

  // calculate and store the static b_level of each node
  def set_blevel ( x: Opr ): Int = {
    if (x.static_blevel < 0) {
      x.static_blevel = if (x.isInstanceOf[TupleOpr]) 0 else size(x)
      if (x.consumers.nonEmpty)
        x.static_blevel += x.consumers.map(c => set_blevel(operations(c))).reduce(Math.max)
      children(x).foreach(c => set_blevel(operations(c)))
    }
    x.static_blevel
  }

  def cpu_cost ( opr: Opr ): Int
    = children(opr).map{ c => val copr = operations(c)
                              copr match {
                                case LoadOpr(_,_) => 0
                                case TupleOpr(_,_) => 0
                                case _ => size(copr)
                              } }.sum

  def communication_cost ( opr: Opr, node: WorkerID ): Int
    = children(opr).map{ c => val copr = operations(c)
                              if (copr.node == node)
                                0
                              else size(copr) }.sum

  def schedule[I,T,S] ( e: Plan[I,T,S] ) {
    ready_pool = ListBuffer()
    work = 0.until(Communication.num_of_workers).map(w => 0).toArray
    tasks = 0.until(Communication.num_of_workers).map(w => 0).toArray
    for ( opr <- operations )
       set_blevel(opr)
    for ( op <- 0.until(operations.length)
          if operations(op).isInstanceOf[LoadOpr]
          if operations(op).node < 0
          if !ready_pool.contains(op) )
       ready_pool += op
    while (ready_pool.nonEmpty) {
      // choose a worker that has done the least work
      val w = work.zipWithIndex.minBy{ case (x,i) => (x,tasks(i)) }._2
      // choose opr with the least communication cost; if many, choose one with the highest b_level
      val c = ready_pool.minBy{ x => val opr = operations(x)
                                     ( communication_cost(opr,w),
                                       -opr.static_blevel ) }
      val opr = operations(c)
      // opr is allocated to worker w
      opr.node = w
      work(w) += cpu_cost(opr) + communication_cost(opr,w)
      tasks(w) += 1
      if (trace)
        println("schedule opr "+c+" on node "+w+" (work = "+work(w)+" )")
      ready_pool -= c
      // add more ready nodes
      for { c <- opr.consumers } {
        val copr = operations(c)
        if (copr.node < 0
            && !ready_pool.contains(c)
            && children(copr).forall(operations(_).node >= 0))
          ready_pool += c
      }
    }
    if (trace)
      print_plan(e)
  }


/****************************************************************************************************
* 
* Single core in-memory evalution (for testing only)
* 
****************************************************************************************************/

  var cached_blocks: Int = 0
  var max_cached_blocks: Int = 0

  def eval_mem ( id: OprID, tabs: Int ): Any = {
    val e = operations(id)
    if (e.cached != null) {
      // retrieve result block(s) from cache
      val res = e.cached
      e.count -= 1
      if (e.count <= 0) {
        if (trace)
          println(" "*tabs*3+"* "+"discard the cached value of "+id)
        cached_blocks -= 1
        e.cached = null
      }
      return res
    }
    if (trace)
      println(" "*3*tabs+"*** "+tabs+": "+e)
    val res = e match {
        case LoadOpr(_,b:(()=>Any))
          => b()
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             f(eval_mem(x,tabs+1)).head
        case TupleOpr(x,y)
          => def f ( lg: OprID ): (Any,Any)
               = operations(lg) match {
                    case TupleOpr(x,y)
                      => (f(x),f(y))
                  case _ => eval_mem(lg,tabs+1).asInstanceOf[(Any,Any)]
                 }
             val gx@(iv,_) = f(id)
             (iv,gx)
        case ReduceOpr(s,fid)
          => val sv = s.map(eval_mem(_,tabs+1).asInstanceOf[(Any,Any)])
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    if (trace)
      println(" "*3*tabs+"*-> "+tabs+": "+res)
    e.count -= 1
    if (e.count > 0) {
      e.cached = res
      if (trace)
        println(" "*tabs*3+"* "+"cache the value of "+id)
      cached_blocks += 1
      max_cached_blocks = Math.max(max_cached_blocks,cached_blocks)
    }
    res
  }

  def evalMem[I,T,S] ( e: Plan[I,T,S] ): (T,S,List[Any])
    = e match {
        case (dp,sp,s)
          => cached_blocks = 0
             max_cached_blocks = 0
             operations.foreach(x => x.count = x.consumers.length)
             val res = s.map{ case (i,(ds,ss,lg))
                                => eval_mem(lg,0) }
             println("Number of nodes: "+operations.length)
             println("Max num of cached blocks: "+max_cached_blocks)
             println("Final num of cached blocks: "+cached_blocks)
             (dp,sp,res)
      }
}

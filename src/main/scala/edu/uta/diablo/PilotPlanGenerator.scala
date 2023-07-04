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

sealed abstract class Lineage ( id: Int,
                                var node: Int = -1,        // worker node
                                var cached: Any = null,    // cached result block(s)
                                var count: Int = 0 ) {     // number of parents
  def ID: Int = id
}
case class StoreLineage[T] ( id: Int, index: Any, block: () => T )
     extends Lineage(id)
case class TupleLineage ( id: Int, x: Lineage, y: Lineage )
     extends Lineage(id)
case class ApplyLineage[-T,S] ( id: Int, x: Lineage, fnc: T => S )
     extends Lineage(id)
case class ReduceLineage[-T,S] ( id: Int, x: Lineage, y: Lineage, op: ((T,T)) => S )
     extends Lineage(id)


object PilotPlanGenerator {
  val trace = false
  val newid: Expr = Call("newid",Nil)

  val lineage_type: Type = BasicType("edu.uta.diablo.Lineage")

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
                                                                lineage_type)))))))
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

  def getLineage ( e: Pattern ): Option[Expr] = {
    def gl ( e: Pattern ): Option[Expr]
      = e match {
          case TuplePat(List(key,VarPat(v)))
            => Some(Nth(Var(v),3))
          case TuplePat(List(x,y))
            => for { xl <- gl(x)
                     yl <- gl(y) }
                  yield Call("tupleLineage",List(newid,xl,yl))
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

  def makePlan ( e: Expr ): Expr = {
    def keySize ( x: Expr ): Int
      = elemType(typecheck(x)) match {
          case TupleType(List(BasicType(_),_)) => 1
          case TupleType(List(TupleType(ts),_)) => ts.length
        }
      e match {
        case Nth(Var(v),_)
          if typecheck_var(v).isDefined
          // Scala variable bound to an RDD tensor
          => val i = newvar
             val gl = newvar
             val dp = newvar
             val sp = newvar
             Comprehension(Tuple(List(Var(i),
                              Tuple(List(Var(dp),Var(sp),
                                      Call("storeLineage",
                                           List(newid,Var(i),
                                                Lambda(TuplePat(Nil),
                                                       Tuple(List(Var(i),
                                                            Tuple(List(Var(dp),Var(sp),
                                                                   Var(gl)))))))))))),
                           List(Generator(TuplePat(List(VarPat(i),
                                              TuplePat(List(VarPat(dp),VarPat(sp),VarPat(gl))))),
                                          e)))
        case Nth(Var(v),_)
          // bound to a tensor plan
          => e
        case Call(f,x::y::_)
          if List("diablo_join","diablo_cogroup").contains(f)
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
          if getLineage(p).isDefined && findKey(b).isDefined
          => val k = newvar
             val xl = newvar
             val dp = newvar
             val sp = newvar
             val xp = makePlan(x)
             val Some(gl) = getLineage(p)
             val Some(key) = findKey(b)
             Comprehension(Tuple(List(Var(k),
                                      Tuple(List(Var(dp),Var(sp),
                                                 Call("applyLineage",List(newid,gl,f)))))),
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
                                                        Call("storeLineage",
                                                             List(newid,Var(k),
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
        case MethodCall(x,"reduceByKey",List(op,n))
          => val xl = newvar
             val xdp = newvar
             val xsp = newvar
             val yl = newvar
             val ydp = newvar
             val ysp = newvar
             val xp = makePlan(x)
             Call("reduceByKey",
                  List(xp,Lambda(TuplePat(List(TuplePat(List(VarPat(xdp),VarPat(xsp),
                                                             VarPat(xl))),
                                               TuplePat(List(VarPat(ydp),VarPat(ysp),
                                                             VarPat(yl))))),
                                 Tuple(List(Var(xdp),Var(xsp),
                                            Call("reduceLineage",
                                                 List(newid,Var(xl),Var(yl),op)))))))
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

  def makePilotPlan ( e: Expr ): Expr
    = makePlanExpr(e)

  def setCounts ( e: Lineage ) {
    e.count += 1
    if (e.count > 1)
      return
    e match {
      case ApplyLineage(_,x,_)
        => setCounts(x)
      case TupleLineage(_,x,y)
        => setCounts(x)
           setCounts(y)
      case ReduceLineage(_,x,y,_)
        => setCounts(x)
           setCounts(y)
      case _ => ;
    }
  }

  var cached_blocks: Int = 0
  var max_cached_blocks: Int = 0

  def evalMem ( e: Lineage, tabs: Int ): Any = {
    if (e.cached != null) {
      // retrieve result block(s) from cache
      val res = e.cached
      e.count -= 1
      if (e.count <= 0) {
        if (trace)
          println(" "*tabs*3+"* "+"discard the cached value of "+e.ID)
        cached_blocks -= 1
        e.cached = null
      }
      return res
    }
    if (trace)
      println(" "*3*tabs+"*** "+tabs+": "+e)
    val res = e match {
        case StoreLineage(_,_,b:(()=>Any))
          => b()
        case ApplyLineage(_,x,f:(Any=>List[Any]))
          => f(evalMem(x,tabs+1)).head
        case TupleLineage(_,x,y)
          => def f ( lg: Lineage ): (Any,Any)
               = lg match {
                    case TupleLineage(_,x,y)
                      => (f(x),f(y))
                  case _ => evalMem(lg,tabs+1).asInstanceOf[(Any,Any)]
                 }
             val gx@(iv,_) = f(e)
             (iv,gx)
        case ReduceLineage(_,x,y,op:(((Any,Any))=>Any))
          => val (iv,xv) = evalMem(x,tabs+1).asInstanceOf[(Any,Any)]
             val (_,yv) = evalMem(y,tabs+1).asInstanceOf[(Any,Any)]
             (iv,op((xv,yv)))
      }
    if (trace)
      println(" "*3*tabs+"*-> "+tabs+": "+res)
    e.count -= 1
    if (e.count > 0 && e.cached == null) {
      e.cached = res
      if (trace)
        println(" "*tabs*3+"* "+"cache the value of "+e.ID)
      cached_blocks += 1
      max_cached_blocks = Math.max(max_cached_blocks,cached_blocks)
    }
    res
  }

  def eval[T,S] ( e: (T,S,List[(T,(T,S,Lineage))]) ): (T,S,List[Any])
    = e match {
        case (dp,sp,s)
          => cached_blocks = 0
             s.map{ case (i,(_,_,lg)) => setCounts(lg) }
             val res = (dp,sp,
                        s.map{ case (i,(ds,ss,lg))
                                 => evalMem(lg,1)
                             })
             println("Max cached blocks: "+max_cached_blocks)
             println("Final cached blocks: "+cached_blocks)
             res
      }
}

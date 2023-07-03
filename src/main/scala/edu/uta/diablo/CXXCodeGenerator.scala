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
import java.io._

object CXXCodeGenerator {
  var writer: PrintWriter = _

  var var_count: Int = 0

  var env: Environment = Map()

  def new_var (): String = {
    val v = "_v_"+var_count
    var_count += 1
    v
  }

  def eliminatePattern ( p: Pattern, x: Expr, e: Expr ): Expr
    = p match {
        case VarPat(v)
          => subst(v,x,e)
        case TuplePat(List(q))
          => eliminatePattern(q,x,e)
        case TuplePat(ts)
          if (occurrences(patvars(p),e) > 1) && !Normalizer.isConstant(x)
          => val v = new_var()
             val tp = exprType(x)
             env = env+((v->tp))
             Block(List(VarDecl(v,tp,x),eliminatePattern(p,Var(v),e)))
        case TuplePat(ts)
          => ts.zipWithIndex
               .foldLeft[Expr](e) {
                   case (r,(q,i)) => eliminatePattern(q,Nth(x,i+1),r)
                }
        case _ => e
      }

  def accFlatMapExpr ( e: flatMap ): Expr
    = e match {
        case flatMap(_,_)
          => val v = new_var()
             def store_result ( u: Expr ): Expr
               = u match {
                   case flatMap(Lambda(p,b),x)
                     => flatMap(Lambda(p,store_result(b)),x)
                   case IfE(p,x,y)
                     => IfE(p,store_result(x),store_result(y))
                   case Seq(List(x))
                     => Seq(List(Call(v+".push_back",List(x))))
                   case _ => u
                 }
             val tp = exprType(e)
             env = env+((v->tp))
             // store the result into an array
             Block(List(VarDecl(v,tp,Seq(List(Call(makeCtype(tp),Nil)))),
                        store_result(e),
                        Var(v)))
      }

  def unnestBlocks ( e: Expr, stmt: Boolean ): (List[Expr],Expr) = {
      def unnestBlocksList ( el: List[Expr], stmt: Boolean ): (List[Expr],List[Expr])
        = el.foldLeft[(List[Expr],List[Expr])] (Nil,Nil) {
                case ((ts,te),s)
                  => val (ss,se) = unnestBlocks(s,stmt)
                     ((ts++ss),te:+se)
          }
      def block ( el: List[Expr] ): Expr
        = el.filter{ case Tuple(Nil) => false
                     case Seq(List(Tuple(Nil))) => false
                     case _ => true } match {
            case List(x) => x
            case se => Block(se)
          }
      val none = Tuple(Nil)
      e match {
        case Nth(x,n)
          => val (xs,xe) = unnestBlocks(x,false)
             (xs,Nth(xe,n))
        case Index(x,i)
          => val (xs,xe) = unnestBlocks(x,false)
             val (is,ie) = unnestBlocksList(i,false)
             (xs++is,Index(xe,ie))
        case Block(sl:+x)
          => val (ss,_) = unnestBlocksList(sl,true)
             val (xs,xe) = unnestBlocks(x,stmt)
             (ss++xs,xe)
        case IfE(p,x,y)
          => val (ps,pe) = unnestBlocks(p,false)
             val (xs,xe) = unnestBlocks(x,stmt)
             val (ys,ye) = unnestBlocks(y,stmt)
             if (stmt)
                (ps:+IfE(pe,block(xs:+xe),block(ys:+ye)),none)
             else (ps++xs++ys,IfE(pe,xe,ye))
        case Tuple(el)
          => val (ss,se) = unnestBlocksList(el,false)
             (ss,Tuple(se))
        case Seq(el)
          => val (ss,se) = unnestBlocksList(el,false)
             (ss,Seq(se))
        case Call(f,el)
          => val (ss,se) = unnestBlocksList(el,false)
             (ss,Call(f,se))
        case MethodCall(x,op,el)
          => val (xs,xe) = unnestBlocks(x,false)
             val (ss,se) = unnestBlocksList(el,false)
             (xs++ss,MethodCall(xe,op,se))
        case Let(p,x,b)
          => val tp = exprType(x)
             val (xs,xe) = unnestBlocks(x,false)
             val v = new_var()
             env = env+((v,tp))
             val nb = eliminatePattern(p,Var(v),b)
             val (bs,be) = unnestBlocks(nb,stmt)
             (xs++(VarDecl(v,tp,Seq(List(xe)))::bs),be)
        case Lambda(p,b)
          => val (bs,be) = unnestBlocks(b,stmt)
             (bs,Lambda(p,be))
        case VarDecl(v,tp,x)
          => val (xs,xe) = unnestBlocks(x,false)
             (xs:+VarDecl(v,tp,xe),none)
        case Assign(x,y)
          => val (xs,xe) = unnestBlocks(x,stmt)
             val (ys,ye) = unnestBlocks(y,stmt)
             (xs++ys:+Assign(xe,ye),none)
        case flatMap(Lambda(p,b),x)
          if isRDD(x)
          => val (bs,be) = unnestBlocks(b,stmt)
             val (xs,xe) = unnestBlocks(x,false)
             (xs,flatMap(Lambda(p,block(bs:+be)),xe))
        case flatMap(Lambda(p,b),x)
          if stmt
          => val (bs,be) = unnestBlocks(b,stmt)
             val (xs,xe) = unnestBlocks(x,false)
             (xs:+flatMap(Lambda(p,block(bs:+be)),xe),none)
        case f@flatMap(_,_)
          => val uf = accFlatMapExpr(f)
             unnestBlocks(uf,stmt)
        case _ if stmt
          => (List(e),none)
        case _ => (Nil,e)
      }
  }

  def makeReturn ( e: Expr ): Expr
    = e match {
        case IfE(p,x,Seq(Nil))
          => IfE(p,makeReturn(x),Seq(Nil))
        case IfE(p,x,y)
          => IfE(p,makeReturn(x),makeReturn(y))
        case Block(s:+x)
          => Block(s:+makeReturn(x))
        case _ => Call("return ",List(e))
      }

  def genCfun ( e: Lambda, tp: Type, otp: Type ): String
    = e match {
        case Lambda(p,b)
          => val arg = new_var()
             env = env+((arg->tp))
             val nb = eliminatePattern(p,Var(arg),b)
             val (s,x) = unnestBlocks(nb,false)
             val f = new_var()
             val sc = s.map(makeC(_,2,false))
             val xc = makeC(makeReturn(x),1,true)
             writer.println(makeCtype(otp)+" "+f+" ( "+makeCtype(tp)+" "+arg+" ) {")
             sc.foreach(a => writer.println("   "+a+";"))
             writer.print(xc)
             writer.println(";\n}\n")
             "&"+f
      }

  def makeZero ( tp: Type ): String
    = tp match {
         case BasicType("Int") => "0"
         case BasicType("Long") => "0L"
         case BasicType("Double") => "0.0"
         case BasicType("Boolean") => "false"
         case SeqType(_) => "Nil"
         case TupleType(Nil)
           => "tuple(0)"
         case TupleType(ts)
           => ts.map(makeZero).mkString("make_tuple(",",",")")
         case ArrayType(n,t)
           => val zc = makeZero(t)
              s"vector( $n, $zc )"
         case StorageType(_,_,_)
           => makeZero(unfold_storage_type(tp))
         case _ => "NULL"
      }

  def makeCtype ( tp: Type ): String
    = tp match {
         case BasicType("edu.uta.diablo.EmptyTuple")
           => "nullptr_t"
         case BasicType(nm)
           => nm.toLowerCase
         case TupleType(Nil)
           => "nullptr_t"
         case TupleType(ts)
           => ts.map(makeCtype).mkString("tuple<",",",">")
         case StorageType(_,_,_)
           => makeCtype(unfold_storage_type(tp))
         case SeqType(etp)
           => "vector<"+makeCtype(etp)+">"
         case ParametricType(_,List(etp))
           => "vector<"+makeCtype(etp)+">"
         case ArrayType(n,etp)
           => "Vec<"+makeCtype(etp)+">"
         case _ if tp != null => tp.toString
         case _ => "void*"
      }

  def exprType ( e: Expr ): Type
    = try typecheck(e,env)
      catch { case m: Error
                => //println(m.getMessage);
                   return BasicType("auto") }

  def elemType ( e: Expr ): Type
    = exprType(e) match {
        case ParametricType(_,List(tp))
          => tp
        case SeqType(tp)
          => tp
        case _ => BasicType("auto")
      }

  def tab ( n: Int ): String = "   "*n

  def makeC ( e: Expr, tabs: Int, stmt: Boolean ): String
    = e match {
        case Var(v) => v
        case IntConst(n) => n.toString
        case DoubleConst(n) => n.toString
        case Nth(x,n)
          => "get<"+(n-1)+">("+makeC(x,tabs,false)+")"
        case Index(x,List(n))
          => makeC(x,tabs,false)+"["+makeC(n,tabs,false)+"]"
        case MethodCall(x,op,List(y))
          => "("+makeC(x,tabs,false)+op+makeC(y,tabs,false)+")"
        case Call("merge_tensors",List(x,y,f:Lambda,zero))
          => val tp = exprType(zero)
             "merge_tensors(%s,%s,%s,%s)".format(makeC(x,tabs,false),makeC(y,tabs,false),
                                                 genCfun(f,TupleType(List(tp,tp)),tp),
                                                 makeC(zero,tabs,false))
        case Call(f,es)
          => es.map(makeC(_,tabs,false)).mkString(f+"(",",",")")
        case flatMap(Lambda(VarPat(i),b),Range(n1,n2,IntConst(1)))
          if stmt
          => "for ( int "+i+" = "+makeC(n1,tabs,false)+"; "+i+" <= "+makeC(n2,tabs,false)+"; "+
                i+"++ )\n"+tab(tabs+1)+makeC(b,tabs+1,true)
        case flatMap(Lambda(VarPat(i),b),Range(n1,n2,n3))
          => "for ( int "+i+" = "+makeC(n1,tabs,false)+"; "+i+" <= "+makeC(n2,tabs,false)+"; "+
                i+" += "+makeC(n3,tabs,false)+" )\n"+tab(tabs+1)+makeC(b,tabs+1,true)
        case Tuple(Nil)
          => "nullptr"
        case Tuple(s)
          => s.map(makeC(_,tabs,false)).mkString("make_tuple(",",",")")
        case Seq(Nil)
          => "NULL"
        case Seq(List(x))
          => makeC(x,tabs,stmt)
        case IfE(p,x,Seq(Nil))
          if stmt
          => "if ("+makeC(p,tabs,false)+")\n"+tab(tabs+1)+makeC(x,tabs+1,stmt)
        case IfE(p,x,y)
          if stmt
          => "if ("+makeC(p,tabs,false)+")\n"+tab(tabs+1)+makeC(x,tabs+1,stmt)+"\n"+tab(tabs+1)+
                "else "+makeC(y,tabs+1,stmt)
        case IfE(p,x,y)
          => "(("+makeC(p,tabs,false)+") ? "+makeC(x,tabs,stmt)+" : "+makeC(y,tabs,stmt)+")"
        case Block(Nil)
          => "{ }"
        case Block(List(x,Seq(List(Block(Nil)))))
          => makeC(x,tabs,true)
        case Block(s:+Seq(List(Block(Nil))))
          => s.map(makeC(_,tabs+1,true)).mkString("{ ",";\n"+tab(tabs)," }")
        case Block(s)
          if stmt
          => s.map(makeC(_,tabs+1,true)).mkString("{ ",";\n"+tab(tabs),"; }")
        case Block(s:+x)
          => "({ "+s.map(makeC(_,tabs+1,true)).mkString(";\n"+tab(tabs))+
                  makeC(x,tabs+1,false)+"; })"
        case VarDecl(v,tp,Seq(Nil))
          => val z = makeZero(tp)
             makeCtype(tp)+" "+v+" = "+z
        case VarDecl(v,tp,x)
          => makeCtype(tp)+" "+v+" = "+makeC(x,tabs,false)
        case Assign(d,Seq(List(MethodCall(x,m,List(y)))))
          if x == d && List("+","-","*","/").contains(m)
          => makeC(d,tabs,false)+" "+m+"= "+makeC(y,tabs,false)
        case Assign(d,s)
          => makeC(d,tabs,false)+" = "+makeC(s,tabs,false)
        case Let(p,x,b)
          if (occurrences(patvars(p),b) > 1) && !Normalizer.isConstant(x)
          => val v = new_var()
             val tp = exprType(x)
             env = env+((v->tp))
             "({ "+makeCtype(tp)+" "+v+" = "+makeC(x,tabs,stmt)+";\n   "+
                tab(tabs)+makeC(Let(p,Var(v),b),tabs,stmt)+"; })"
        case Let(p,x,b)
          => makeC(eliminatePattern(p,x,b),tabs,stmt)
        case _ => e.toString
      }

  def isRDD ( e: Expr ): Boolean
    = e match {
        case Call("diablo_join",_) => true
        case flatMap(_,x) => isRDD(x)
        case MethodCall(x,"reduceByKey",_) => true
        case MethodCall(_,"parallelize",_) => true
        case Nth(Var(_),3) => true
        case _ => elemType(e) == BasicType(collectionClass)
      }

  def makeSparkC ( e: Expr ): String
    = e match {
        case Var(v)
          => v
        case Call("diablo_join",List(x,y,nx,ny,_))
          => val xp = makeSparkC(x)
             val yp = makeSparkC(y)
             "join("+xp+","+yp+")"
        case flatMap(f,x)
          if isRDD(x)
          => val xp = makeSparkC(x)
             val tp = elemType(x)
             val otp = elemType(e)
             val fp = genCfun(f,tp,otp)
             "flatMap("+fp+","+xp+")"
        case MethodCall(x,"reduceByKey",List(f:Lambda,_))
          => val xp = makeSparkC(x)
             val TupleType(List(_,tp)) = elemType(x)
             val fp = genCfun(f,TupleType(List(tp,tp)),tp)
             "reduceByKey("+xp+","+fp+")"
        case MethodCall(_,"parallelize",x::_)
          => "parallelize("+makeC(x,0,false)+")"
        case Block(s)
          => s.map(makeSparkC).mkString("{ ",";\n   ","; }")
        case VarDecl(v,tp,x)
          => makeCtype(tp)+" "+v+" = "+makeSparkC(x)
        case Tuple(Nil)
          => "nullptr"
        case Tuple(s)
          => s.map(makeSparkC).mkString("make_tuple(",",",")")
        case Seq(List(x))
          => makeSparkC(x)
        case _ => makeC(e,0,false)
      }

  def makeSparkCode ( e: Expr, print_writer: PrintWriter ) {
    writer = print_writer
    e match {
      case Block(xs)
        => xs.foreach {
              xe => val (s,se) = unnestBlocks(xe,true)
              s.foreach {
                case x@VarDecl(_,_,_)
                  => writer.println(makeSparkC(x)+";\n")
                case x
                  => val v = new_var()
                     val f = new_var()
                     writer.println("int "+f+" () {\n"+makeSparkC(x)
                                    +";\nreturn 0;\n}\n\nint "+v+" = "+f+"();\n")
              }
              se match {
                case Tuple(Nil) => ;
                case Seq(List(Tuple(Nil))) => ;
                case _
                  => val v = new_var()
                     val f = new_var()
                     writer.println("int "+f+" () {\n"+makeSparkC(se)
                                    +";\nreturn 0;\n}\n\nint "+v+" = "+f+"();\n")
              }
           }
    }
  }
}

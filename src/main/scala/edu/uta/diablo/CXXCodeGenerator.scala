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
import java.io._

object CXXCodeGenerator {
  var writer: PrintWriter = _

  var var_count: Int = 0

  var env: Environment = Map()

  val oprIDtype = "edu.uta.diablo.PlanGenerator.OprID"

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
             env = env + ((v->tp))
             Block(List(VarDecl(v,tp,x),eliminatePattern(p,Var(v),e)))
        case TuplePat(ts)
          => ts.zipWithIndex
               .foldLeft[Expr](e) {
                   case (r,(q,i)) => eliminatePattern(q,Nth(x,i+1),r)
                }
        case _ => e
      }

  def exprType ( e: Expr ): Type
    = try typecheck(e,env)
      catch { case m: Error
                => println("Warning: cannot get the type of "+e+"\n"+m.getMessage);
                   return BasicType("auto") }

  def elemType ( e: Expr ): Type
    = exprType(e) match {
        case ParametricType(_,List(tp))
          => tp
        case SeqType(tp)
          => tp
        case tp => BasicType("auto")
      }

  def isCollection ( tp: Type ): Boolean
    = tp match {
        case SeqType(_) => true
        case ArrayType(_,_) => true
        case _ => false
      }

  def makeReturn ( e: Expr, dest: String, tp: Type ): Expr
    = e match {
        case IfE(p,x,Seq(Nil))
          => IfE(p,makeReturn(x,dest,tp),Seq(Nil))
        case IfE(p,x,y)
          => IfE(p,makeReturn(x,dest,tp),makeReturn(y,dest,tp))
        case Let(p,x,y)
          => Let(p,x,makeReturn(y,dest,tp))
        case Block(s:+x)
          => Block(s:+makeReturn(x,dest,tp))
        case Tuple(Nil)
          => Tuple(Nil)
        case Seq(Nil)
          => Tuple(Nil)
        case Seq(List(u))
          => if (isCollection(tp))
               Call("append1",List(Var(dest),u))
             else Assign(Var(dest),u)
        case _
          => if (isCollection(tp))
               Call("append",List(Var(dest),e))
             else Assign(Var(dest),e)
      }

  def makeZero ( tp: Type ): Expr
    = tp match {
         case BasicType("Int")
           => IntConst(0)
         case BasicType("Long")
           => LongConst(0L)
         case BasicType("Double")
           => DoubleConst(0.0)
         case BasicType("Boolean")
           => BoolConst(false)
         case TupleType(List(t))
           => makeZero(t)
         case TupleType(ts)
           => Tuple(ts.map(makeZero))
         case ArrayType(n,t)
           => val tc = makeCtype(t)
              Coerce(Call("new Vec<"+tc+">",Nil),tp)
         case SeqType(t)
           => val tc = makeCtype(t)
              Coerce(Call("new vector<"+tc+">",Nil),tp)
         case StorageType(_,_,_)
           => makeZero(unfold_storage_type(tp))
         case _ => Var("nullptr")
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
        case Block(sl)
          if stmt
          => val (ss,se) = unnestBlocksList(sl,true)
             (ss++se,none)
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
          if stmt
          => val (ss,se) = unnestBlocksList(el,false)
             (ss:+Call(f,se),none)
        case Call(f,el)
          => val (ss,se) = unnestBlocksList(el,false)
             (ss,Call(f,se))
        case Coerce(x,tp)
          => val (xs,xe) = unnestBlocks(x,stmt)
             (xs,Coerce(xe,tp))
        case MethodCall(x,op,null)
          => val (xs,xe) = unnestBlocks(x,false)
             (xs,MethodCall(xe,op,null))
        case MethodCall(x,op,el)
          => val (xs,xe) = unnestBlocks(x,false)
             val (ss,se) = unnestBlocksList(el,false)
             (xs++ss,MethodCall(xe,op,se))
        case Let(p,x,b)
          => val tp = exprType(x)
             val v = new_var()
             env = env + ((v,tp))
             val (xs,xe) = unnestBlocks(x,false)
             val nb = eliminatePattern(p,Var(v),b)
             val (bs,be) = unnestBlocks(nb,stmt)
             (xs++(VarDecl(v,tp,Seq(List(xe)))::bs),be)
        case Lambda(p,b)
          => val (bs,be) = unnestBlocks(b,stmt)
             (bs,Lambda(p,be))
        case VarDecl(v,tp,null)
          => env = env + ((v,tp))
             (List(e),none)
        case VarDecl(v,tp,x)
          => env = env + ((v,tp))
             val (xs,xe) = unnestBlocks(x,false)
             (xs:+VarDecl(v,tp,xe),none)
        case Assign(x,y)
          => val (xs,xe) = unnestBlocks(x,false)
             val (ys,ye) = unnestBlocks(y,false)
             (xs++ys:+Assign(xe,ye),none)
        case While(p,x)
          => val (ps,pe) = unnestBlocks(p,false)
             val (xs,xe) = unnestBlocks(x,true)
             (ps:+While(pe,block(xs:+xe)),none)
        case flatMap(Lambda(p,b),x)
          if stmt
          => val v = new_var()
             val tp = elemType(x)
             env = env + ((v,tp))
             val nb = eliminatePattern(p,Var(v),b)
             val (xs,xe) = unnestBlocks(x,false)
             val (bs,be) = unnestBlocks(nb,stmt)
             (xs:+Call("for",List(VarDecl(v,tp,xe),
                                  block(bs:+be))),
              none)
        case flatMap(Lambda(p,b),x)
          => val v = new_var()
             val w = new_var()
             val tp = elemType(x)
             env = env + ((v,tp))
             val otp = exprType(e)
             env = env + ((w,otp))
             val nb = eliminatePattern(p,Var(v),b)
             val (xs,xe) = unnestBlocks(x,false)
             val (bs,be) = unnestBlocks(nb,stmt)
             val ret = makeReturn(be,w,otp)
             (xs++List(VarDecl(w,otp,null),
                       Call("for",List(VarDecl(v,tp,xe),
                                       block(bs:+ret)))),
              Var(w))
        case _ if stmt
          => (List(e),none)
        case _ => (Nil,e)
      }
  }

  def genCfun ( e: Lambda, tp: Type, otp: Type ): String
    = e match {
        case Lambda(p,b)
          => val arg = new_var()
             val v = new_var()
             env = env + ((arg->tp))
             env = env + ((v->otp))
             val nb = eliminatePattern(p,Var(arg),b)
             val (s,x) = unnestBlocks(nb,false)
             val f = new_var()
             val sc = s.map(makeC(_,2,true))
             val ret = makeReturn(x,v,otp)
             val xc = makeC(ret,2,true)
             writer.println(makeCtype(otp)+" "+f+" ( "+makeCtype(tp)+" "+arg+" ) {")
             writer.println("   "+makeC(VarDecl(v,otp,null),2,true)+";")
             sc.foreach(a => writer.println("   "+a+";"))
             writer.print("   "+xc)
             writer.print(";\n   return "+v+";\n}\n\n")
             "&"+f
      }

  def makeCtype ( tp: Type ): String
    = tp match {
         case BasicType("edu.uta.diablo.EmptyTuple")
           => "nullptr_t"
         case BasicType(nm)
           if nm == oprIDtype
           => "int"
         case BasicType(nm)
           => nm.toLowerCase
         case TupleType(Nil)
           => "nullptr_t"
         case TupleType(List(t))
           => makeCtype(t)
         case TupleType(ts)
           => ts.map(makeCtype).mkString("tuple<",",",">*")
         case StorageType(_,_,_)
           => makeCtype(unfold_storage_type(tp))
         case SeqType(etp)
           => "vector<"+makeCtype(etp)+">*"
         case ParametricType(_,List(etp))
           => "vector<"+makeCtype(etp)+">*"
         case ArrayType(n,etp)
           => "Vec<"+makeCtype(etp)+">*"
         case _ if tp != null => tp.toString
         case _ => "void*"
      }

  def binary_oprs = Map( "+" -> "+", "-" -> "-", "*" -> "*", "/" -> "/", "%" -> "%",
                         "==" -> "==", "<" -> "<", ">" -> ">", "<=" -> "<=", ">=" -> ">=",
                         "!=" -> "!=", "&&" -> "&&", "||" -> "||" )

  def tab ( n: Int ): String = "   "*n


  def get_arrays ( e: Expr, exclude: List[String] ): Map[String,Expr]
    = e match {
        case Index(a,i)
          if freevars(a).intersect(exclude).isEmpty
          => Map(new_var() -> a)
        case _ => accumulate[Map[String,Expr]](e,get_arrays(_,exclude),_++_,Map())
      }

  def get_arrays ( e: Expr ): Map[String,Expr] = {
    def excl ( e: Expr ): List[String]
      = e match {
          case Let(VarPat(v),x,u)
            => v::excl(x)++excl(u)
          case VarDecl(v,_,u)
            => v::excl(u)
          case _ => accumulate[List[String]](e,excl,_++_,Nil)
        }
    get_arrays(e,excl(e))
  }

  def makeC ( e: Expr, tabs: Int, stmt: Boolean ): String
    = e match {
        case Var(v) => v
        case IntConst(n) => n.toString
        case DoubleConst(n) => n.toString
        case BoolConst(n) => n.toString
        case Nth(x,n)
          => "get<"+(n-1)+">(*"+makeC(x,tabs,false)+")"
        case Index(Var(v),List(n))
          => v+"["+makeC(n,tabs,false)+"]"
        case Index(x,List(n))
          => "(*"+makeC(x,tabs,false)+")["+makeC(n,tabs,false)+"]"
        case MethodCall(x,op,List(y))
          if binary_oprs.contains(op)
          => "("+makeC(x,tabs,false)+binary_oprs(op)+makeC(y,tabs,false)+")"
        case Call("args",List(x))
          => "argv["+makeC(x,tabs,false)+"+1]"
        case Call("vector",List(n,v))
          => val nc = makeC(n,tabs,false)
             val vc = makeC(v,tabs,false)
             val tc = makeCtype(exprType(v))
             "new vector<"+tc+">("+nc+","+vc+")"
        case MethodCall(x,"length",null)
          => makeC(x,tabs,false)+".size()"
        case MethodCall(x,"toInt",null)
          => "atoi("+makeC(x,tabs,false)+")"
        case MethodCall(x,"toList",null)
          => makeC(x,tabs,false)
        case MethodCall(x,"par",null)
          => makeC(x,tabs,false)
        case MethodCall(x,"reduceByKey",List(op:Lambda))
          => val TupleType(List(_,tp)) = elemType(Nth(x,3))
             val fp = genCfun(op,TupleType(List(tp,tp)),tp)
             "reduceByKey(" + makeC(x,tabs,false)+","+fp+")"
        case MethodCall(_,m,_)
          => throw new Error("Don't know how to compile method "+m)
        case Call("merge_tensors",List(x,y,f:Lambda,zero))
          => val tp = exprType(zero)
             ("merge_tensors(%s,%s,%s,%s)"
                 .format(makeC(x,tabs,false),makeC(y,tabs,false),
                         genCfun(f,TupleType(List(tp,tp)),tp),
                         makeC(zero,tabs,false)))
        case Call("for",List(VarDecl(i,tp,MethodCall(Range(n1,n2,n3),"par",null)),b))
          => val m = get_arrays(b)
             val nb = m.foldLeft[Expr](b){ case (r,(v,u)) => subst(u,Var(v),r) }
             "{ "+m.map{ case (v,u) => "auto "+v+" = "+makeC(u,tabs,false)+"->buffer(); " }.mkString("")+"\n"+
              tab(tabs-1)+"#pragma omp parallel for\n" +
              tab(tabs-1)+"for ( int "+i+" = "+makeC(n1,tabs,false)+"; "+i+
                 " <= "+makeC(n2,tabs,false)+"; "+i+" += "+makeC(n3,tabs,false)+" )\n"+
                 tab(tabs)+makeC(nb,tabs+1,true)+" }"
        case Call("for",List(VarDecl(i,tp,Range(n1,n2,n3)),b))
          => "for ( int "+i+" = "+makeC(n1,tabs,false)+"; "+i+
                 " <= "+makeC(n2,tabs,false)+"; "+i+" += "+makeC(n3,tabs,false)+" )\n"+
                 tab(tabs)+makeC(b,tabs+1,true)
        case Call("for",List(VarDecl(v,tp,x),b))
          => "for ( "+makeCtype(tp)+" "+v+": *"+makeC(x,tabs,false)+" )\n" +
                tab(tabs)+makeC(b,tabs+1,true)
        case Call(f,es)
          => es.map(makeC(_,tabs,false)).mkString(f+"(",",",")")
        case Range(n1,n2,n3)
          => "range("+makeC(n1,tabs,false)+","+makeC(n2,tabs,false)+
                 ","+makeC(n3,tabs,false)+")"
        case Tuple(Nil)
          => "nullptr"
        case Tuple(List(x))
          => makeC(x,tabs,false)
        case Tuple(s)
          => val ts = s.map( x => makeCtype(exprType(x)) ).mkString("<",",",">")
             s.map(makeC(_,tabs,false)).mkString("new tuple"+ts+"(",",",")")
        case Seq(Nil)
          => "nullptr"
        case Seq(List(x))
          => "elem("+makeC(x,tabs,stmt)+")"
        case Seq(s)
          => val tc = makeCtype(exprType(s.head))
             "new vector<"+tc+">({ "+s.map(makeC(_,tabs,false)).mkString(", ")+" })"
        case IfE(p,x,Seq(Nil))
          if stmt
          => "{ assert("+makeC(p,tabs,false)+");\n"+tab(tabs)+makeC(x,tabs+1,stmt)+"; }"
        case IfE(p,x,Seq(Nil))
          if stmt
          => "if ("+makeC(p,tabs,false)+")\n"+tab(tabs)+makeC(x,tabs+1,stmt)
        case IfE(p,x,y)
          if stmt
          => "if ("+makeC(p,tabs,false)+")\n"+tab(tabs+1)+makeC(x,tabs+1,stmt)+"\n"+
                 tab(tabs)+"else "+makeC(y,tabs+1,stmt)
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
                  (if (s.isEmpty) "" else (";\n"+tab(tabs)))+makeC(x,tabs+1,false)+"; })"
        case VarDecl(v,tp@SeqType(et),null)
          => env = env + ((v,tp))
             makeCtype(tp)+" "+v+" = new vector<"+makeCtype(et)+">()"
        case VarDecl(v,tp,null)
          => env = env + ((v,tp))
             makeCtype(tp)+"  "+v
        case VarDecl(v,tp,Seq(Nil))
          => env = env + ((v,tp))
             val z = makeC(makeZero(tp),tabs,false)
             makeCtype(tp)+" "+v+" = "+z
        case VarDecl(v,tp,Seq(List(x)))
          => env = env + ((v,tp))
             makeCtype(tp)+" "+v+" = "+makeC(x,tabs,false)
        case VarDecl(v,tp,x)
          => env = env + ((v,tp))
             makeCtype(tp)+" "+v+" = "+makeC(x,tabs,false)
        case Assign(d,Seq(List(MethodCall(x,m,List(y)))))
          if x == d && List("+","-","*","/").contains(m)
          => makeC(d,tabs,false)+" "+m+"= "+makeC(y,tabs,false)
        case Assign(d,Seq(List(s)))
          => makeC(d,tabs,false)+" = "+makeC(s,tabs,false)
        case Assign(d,s)
          => makeC(d,tabs,false)+" = "+makeC(s,tabs,false)
        case Let(p,x,b)
          if (occurrences(patvars(p),b) > 1) && !Normalizer.isConstant(x)
          => val v = new_var()
             val tp = exprType(x)
             env = env + ((v->tp))
             "({ "+makeCtype(tp)+" "+v+" = "+makeC(x,tabs,stmt)+";\n   "+
                tab(tabs)+makeC(Let(p,Var(v),b),tabs,stmt)+"; })"
        case Let(p,x,b)
          => makeC(eliminatePattern(p,x,b),tabs,stmt)
        case While(p,b)
          => "while ("+makeC(p)+")\n"+makeC(b,tabs,true)+"\n"
        case Coerce(x,_)
          => makeC(x,tabs,false)
        case le@Lambda(_,_)
          => val FunctionType(tp,otp) = exprType(e)
             genCfun(le,tp,otp)
        case _ => e.toString
      }

  def makeC ( e: Expr ): String
    = e match {
        case Seq(List(x))
          => makeC(x,0,false)
        case _ => makeC(e,0,false)+"[0]"
      }

  def makeCxxCode ( e: Expr ): String = {
    var main_block = ""
    e match {
      case Block(xs)
        => val (s,se) = unnestBlocks(e,false)
           s.foreach {
              case VarDecl(v,tp,u@Seq(List(_)))
                => env = env + ((v,tp))
                   main_block = main_block + v+" = "+makeC(u)+";\n"
                   writer.println(makeC(VarDecl(v,tp,null),0,true)+";\n")
              case VarDecl(v,tp,null)
                => env = env + ((v,tp))
                   writer.println(makeC(VarDecl(v,tp,null),0,true)+";\n")
              case Tuple(Nil) => ;
              case Seq(List(x))
                => val f = new_var()
                   main_block = main_block + f+"();\n"
                   writer.println("void "+f+" () {\n   "+makeC(x,1,true)+";\n}\n\n")
              case x
                => val f = new_var()
                   main_block = main_block + f+"();\n"
                   writer.println("void "+f+" () {\n   "+makeC(x,1,true)+";\n}\n\n")
           }
           se match {
              case Tuple(Nil) => ;
              case Seq(List(Tuple(Nil))) => ;
              case _
                => val v = new_var()
                   val t = new_var()
                   val f = new_var()
                   main_block = main_block + "for ( auto "+v+": *"+f+"() ) { schedule("+
                                    v+"); eval("+v+"); collect("+v+"); }\n"
                   writer.println("auto "+f+" () {\n   return "+makeC(se,0,false)+";\n}\n\n")
           }
    }
    main_block
  }

  def genCxxCode ( e: Expr, functions: List[Expr], print_writer: PrintWriter ) {
    writer = print_writer
    writer.println("#include \"runtime.h\"\n")
    val s = makeCxxCode(e)
    val fs = functions.map(f => "functions.push_back((void*(*)(void*))"
                                +makeC(f,0,false)+");\n").mkString("")
    writer.println(s"int main ( int argc, char* argv[] ) {\nmpi_startup(argc,argv);\n$fs${s}mpi_finalize();\nreturn 0;\n}")
  }
}

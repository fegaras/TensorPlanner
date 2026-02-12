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
import Math.max

object CXXCodeGenerator {
  var writer: PrintWriter = _
  var mlir_writer: PrintWriter = _

  var var_count: Int = 0

  var env: Environment = Map()

  val oprIDtype = "edu.uta.diablo.PlanGenerator.OprID"

  def max ( x: Int, y: Int ) = Math.max(x,y)
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

  def reorderFlatMaps ( e: Expr ): Expr
    = e match {
      case flatMap(Lambda(q,c),y)
        => c match {
          case flatMap(Lambda(r,d),z)
            if(!get_vars(z).contains(get_vars(toExpr(q)).head))
            => flatMap(Lambda(r,flatMap(Lambda(q,d),y)),z)
          case Block(s)
            => {
              var inner_fm = Option.empty[Expr]
              val ns = s.map {
                case fm@flatMap(Lambda(r,d),z)
                  => inner_fm = Some(fm)
                      d
                case other => other
              }
              inner_fm match {
                case Some(flatMap(Lambda(r,d),z))
                  => flatMap(Lambda(r,flatMap(Lambda(q,Block(ns)),y)),z)
                case _ => e
              }
            }
          case Let(p,nv,IfE(mc,b,nb))
            => b match {
              case flatMap(Lambda(r,d),z)
               if(!get_vars(z).contains(get_vars(toExpr(q)).head))
                => flatMap(Lambda(r,flatMap(Lambda(q,Let(p,nv,IfE(mc,d,nb))),y)),z)
              case _ => e
            }
          case _ => e
        }
      case _ => e
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
          => val b1 = x match {
               case MethodCall(_, "par", _)
                 => reorderFlatMaps(b)
               case _ => b
             }
             val v = new_var()
             val tp = elemType(x)
             env = env + ((v,tp))
             val nb = eliminatePattern(p,Var(v),b1)
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

  def makeCtype ( tp: Type, in_coords: Boolean = false ): String
    = tp match {
         case BasicType("edu.uta.diablo.EmptyTuple")
           => "nullptr_t"
         case BasicType(nm)
           if nm == oprIDtype
           => "int"
         case BasicType("Int")
           if in_coords
           => "uintptr_t"
         case BasicType("Double")
           => "float"
         case BasicType(nm)
           => nm.toLowerCase
         case TupleType(Nil)
           => "nullptr_t"
         case TupleType(List(t))
           => makeCtype(t,true)
         case TupleType(ts)
           => ts.map(makeCtype(_,true)).mkString("tuple<",",",">*")
         case StorageType(_,_,_)
           => makeCtype(unfold_storage_type(tp))
         case SeqType(etp)
           => "vector<"+makeCtype(etp,true)+">*"
         case ParametricType(_,List(etp))
           => "vector<"+makeCtype(etp,true)+">*"
         case ArrayType(n,etp)
           => "Vec<"+makeCtype(etp)+">*"
         case _ if tp != null => tp.toString
         case _ => "void*"
      }

  def binary_oprs = Map( "+" -> "+", "-" -> "-", "*" -> "*", "/" -> "/", "%" -> "%",
                         "==" -> "==", "<" -> "<", ">" -> ">", "<=" -> "<=", ">=" -> ">=",
                         "!=" -> "!=", "&&" -> "&&", "||" -> "||" )

  def arith_oprs = Map( "+" -> "arith.addi", "-" -> "arith.subi", "*" -> "arith.muli", "/" -> "arith.divsi", "%" -> "arith.remsi",
                         "==" -> "arith.cmpi eq", "<" -> "arith.cmpi slt", ">" -> "arith.cmpi sgt", "<=" -> "arith.cmpi sle",
                         ">=" -> "arith.cmpi sge", "!=" -> "arith.cmpi ne", "&&" -> "arith.andi", "||" -> "arith.ori" )

  def tab ( n: Int ): String = "   "*n


  def get_arrays ( e: Expr, exclude: List[String] ): Map[String,Expr]
    = e match {
        case Index(a,i)
          if freevars(a).intersect(exclude).isEmpty
          => i.foldLeft(Map[String,Expr]()) {case (accumulator,ii) => accumulator++get_arrays(ii,exclude)} ++ Map(new_var() -> a)
        case Nth(x,i)
          if freevars(e).intersect(exclude).isEmpty
          => Map(new_var() -> e)
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

  def get_vars ( expr: Expr ): List[String]
    = expr match {
        case Var(v)
          => List(v)
        case Index(Var(v),List(n))
          => get_vars(n)
        case MethodCall(x,_,List(y))
          => get_vars(x)++get_vars(y)
        case _ => List()
      }

  def get_array_indices ( e : Expr): List[String] = {
    e match {
      case Assign(d,_)
        => get_vars(d)
      case Call("for",List(_,b))
        => get_array_indices(b)
      case Block(s:+Seq(List(Block(Nil))))
        => s.flatMap(get_array_indices(_))
      case Block(s)
        => s.flatMap(get_array_indices(_))
      case IfE(p,x,y)
        => get_array_indices(x) ++ get_array_indices(y)
      case _ => List()
    }
  }

  def makeSSA ( e: Expr): String = {
    var ssa_decls = ""
    def makeSSAUtil( expr: Expr ): String = {
      expr match {
        case Var(v) => "%"+v
        case IntConst(n) => n.toString
        case DoubleConst(n) => n.toString
        case BoolConst(n) => n.toString
        case Index(Var(v),List(n))
          => "%"+v+"["+makeSSAUtil(n)+"]"
        case MethodCall(x,op,List(y))
          if arith_oprs.contains(op)
          => val x_v = makeSSAUtil(x)
            val y_v = makeSSAUtil(y)
            val new_v = new_var()
            env = env + ((new_v,exprType(x)))
            ssa_decls += "%"+new_v+" = "+arith_oprs(op)+" "+x_v+", "+y_v+" : index\n"
            "%"+new_v
        case IfE(p,x,y)
          => val new_v = new_var()
            env = env + ((new_v,exprType(y)))
            val np = makeSSAUtil(p)
            val x_v = makeSSAUtil(x)
            val y_v = makeSSAUtil(y)
            ssa_decls += "%"+new_v+" = affine.if "+np+" {\n"+x_v+"\n} else {\n"+y_v+"\n}\n"
            "%"+new_v
        case Assign(d,Seq(List(MethodCall(x,m,List(y)))))
          if x == d && List("+","-","*","/").contains(m)
          => val nd = makeSSAUtil(d)
            val ny = makeSSAUtil(y)
            val new_v = new_var()
            env = env + ((new_v,exprType(y)))
            ssa_decls += "%"+new_v+" = "+arith_oprs(m)+" "+nd+", "+ny+" : index\n"
            "%"+new_v
        case Assign(d,Seq(List(s)))
          => makeSSAUtil(d)+" = "+makeSSAUtil(s)
        case Assign(d,s)
          => makeSSAUtil(d)+" = "+makeSSAUtil(s)
        case _ => expr.toString
      }
    }
    val ssa_expr = makeSSAUtil(e)
    ssa_decls + ssa_expr
  }

  def makeMLIR( e: Expr, tabs: Int, stmt: Boolean ): String = {
    e match {
      case Var(v) => "%"+v
      case IntConst(n) => n.toString
      case DoubleConst(n) => n.toString
      case BoolConst(n) => n.toString
      case Index(Var(v),List(n))
        => makeSSA(e)
      case MethodCall(x,op,List(y))
        => makeSSA(e)
      case Call("for",List(VarDecl(i,tp,MethodCall(Range(n1,n2,n3),"par",null)),b))
        => val m = get_arrays(b)
        var nb = m.foldLeft[Expr](b){ case (r,(v,u)) => subst(u,Var(v),r) }
        "affine.for %"+i+" = 0 to "+block_dim_size+"{\n"+tab(tabs+1)+makeMLIR(nb,tabs,false) +"}\n"
      case Call("for",List(VarDecl(i,tp,Range(n1,n2,n3)),b))
        => "affine.for %"+i+" = 0 to "+block_dim_size+"{\n"+tab(tabs+1)+makeMLIR(b,tabs,false) +"}\n"
      case Block(List(x,Seq(List(Block(Nil)))))
        => makeMLIR(x,tabs,true)
      case Block(s:+Seq(List(Block(Nil))))
        => s.map(makeMLIR(_,tabs+1,true)).mkString("{ ","\n"+tab(tabs),"}")
      case Block(s)
        if stmt
        => s.map(makeMLIR(_,tabs+1,true)).mkString("{ ","\n"+tab(tabs),"}")
      case Block(s:+x)
        => "({ "+s.map(makeMLIR(_,tabs+1,true)).mkString("\n"+tab(tabs))+
                (if (s.isEmpty) "" else ("\n"+tab(tabs)))+makeMLIR(x,tabs+1,false)+"})"
      case Assign(_,_)
        => makeSSA(e)
      case _ => e.toString
    }
  }

  val gpu_block_x = 512
  val gpu_block_y = 512

  def writeMLIR_gemm( mlir_func_args: List[String]): String = {
    val data_type = "f32"
    val matrix_type = s"memref<${block_dim_size}x${block_dim_size}x${data_type}>"
    val dimx = block_dim_size/gpu_block_x
    val dimy = block_dim_size/gpu_block_y
    val mlir_code = s"""
    %c1 = arith.constant 1 : index
    %c2 = arith.constant ${dimx} : index
    %c${gpu_block_x} = arith.constant ${gpu_block_x} : index
    gpu.launch blocks(%bx, %by, %bz) in (%grid_x = %c${gpu_block_x}, %grid_y = %c1, %grid_z = %c1)
                threads(%tx, %ty, %tz) in (%block_x = %c${gpu_block_x}, %block_y = %c1, %block_z = %c1) {
      %i = arith.muli %c2, %bx : index
      %j = arith.muli %c2, %tx : index
      %a_smem = memref.get_global @a_smem_global : memref<${dimx}x128xf32, 3>
      %b_smem = memref.get_global @b_smem_global : memref<128x${dimy}xf32, 3>
      affine.for %k = 0 to ${block_dim_size} step 128 {
        affine.for %copyii = #map0(%i) to #map2(%i) {
          affine.for %copykk = #map0(%k) to #map1(%k) {
            %0 = affine.load %arg0[%copyii, %copykk] : ${matrix_type}
            affine.store %0, %a_smem[%copyii - %i, %copykk - %k] : memref<${dimx}x128xf32, 3>
          }
        }
        affine.for %copykk = #map0(%k) to #map1(%k) {
          affine.for %copyjj = #map0(%j) to #map2(%j) {
            %0 = affine.load %arg1[%copykk, %copyjj] : ${matrix_type}
            affine.store %0, %b_smem[%copykk - %k, %copyjj - %j] : memref<128x${dimy}xf32, 3>
          }
        }
        affine.for %ii = 0 to ${dimx} {
          affine.for %jj = 0 to ${dimy} {
            %0 = affine.apply #map3(%i, %ii)
            %1 = affine.apply #map3(%j, %jj)
            %2 = affine.load %arg2[%0, %1] : ${matrix_type}
            %res = affine.for %kk = 0 to 128 iter_args(%accum = %2) -> (f32) {
              %3 = affine.load %a_smem[%ii, %kk] : memref<${dimx}x128xf32, 3>
              %4 = affine.load %b_smem[%kk, %jj] : memref<128x${dimy}xf32, 3>
              %5 = arith.mulf %3, %4 : f32
              %6 = arith.addf %accum, %5 : f32
              affine.yield %6 : f32
            }
            affine.store %res, %arg2[%0, %1] : ${matrix_type}
          }
        }
      }
      gpu.terminator
    }
    """
    mlir_code
  }

  def writeMLIR( e: Expr, mlir_func_args: List[String] ): String = {
    val data_type = "f32"
    val matrix_type = s"memref<${block_dim_size}x${block_dim_size}x${data_type}>"
    val args = mlir_func_args.map(arg => s"%${arg}: ${matrix_type}").mkString(", ")
    val matmul_args = mlir_func_args.map(arg => s"%${arg}").mkString(", ")
    val mlir_func_name = new_var()
    val dimx = block_dim_size/gpu_block_x
    val dimy = block_dim_size/gpu_block_y
    val mlir_func = s"""
    func.func @${mlir_func_name}(%arg2 : ${matrix_type}, %arg1 : ${matrix_type}, %arg0 : ${matrix_type}) -> ${matrix_type} {
    ${writeMLIR_gemm(mlir_func_args)}\n
    return %arg2 : ${matrix_type}\n}
    """
    mlir_writer.println(mlir_func)
    val mlir_func_call = s"""
    CUmodule cuModule;
    CUfunction cuFunction;
    std::string ptx = loadPTX(\"mlir_output.ptx\");
    cuModuleLoadDataEx(&cuModule, ptx.c_str(), 0, 0, 0);
    cuModuleGetFunction(&cuFunction, cuModule, \"${mlir_func_name}_kernel\");
    launchCudaKernel(cuFunction, device_id, ${mlir_func_args.mkString(", ")});\n"""
    mlir_func_call
  }

  def makeC ( e: Expr, tabs: Int, stmt: Boolean ): String = {
    def has_reduction( expr : Expr): Boolean
      = expr match {
        case Call("for",List(_,blk))
          => has_reduction(blk)
        case Assign(d,Seq(List(MethodCall(x,m,List(y)))))
          => x == d && m == "+" // Checking for +/ only
        case Block(s:+Seq(List(Block(Nil))))
          => s.map(has_reduction(_)).reduce( (x:Boolean, y:Boolean) => x || y)
        case Block(s)
          => s.map(has_reduction(_)).reduce( (x:Boolean, y:Boolean) => x || y)
        case IfE(p,x,y)
          => has_reduction(x) || has_reduction(y)
        case _ => false
      }

    def get_nested_for_loop ( expr: Expr ): Option[Expr]
      = expr match {
        case Call("for",_)
          => Some(expr)
        case Block(s:+Seq(List(Block(Nil))))
          => s.map(get_nested_for_loop(_)).find( x => x.isDefined ).flatten
        case Block(s)
          => s.map(get_nested_for_loop(_)).find( x => x.isDefined ).flatten
        case IfE(p,x,y)
          => get_nested_for_loop(x) orElse get_nested_for_loop(y)
        case _ => None
      }

    def remove_for_loop ( expr: Expr ): Expr
      = expr match {
        case Call("for",List(_,block))
          => block
        case Block(s:+Seq(List(Block(Nil))))
          => Block(s.map(remove_for_loop(_)))
        case Block(s)
          => Block(s.map(remove_for_loop(_)))
        case IfE(p,x,y)
          => IfE(p,remove_for_loop(x),remove_for_loop(y))
        case _ => expr
      }

    def reorder_loops ( expr: Expr ): Expr
    = expr match {
        case Call("for",List(VarDecl(j,tpv,Range(n1,n2,n3)),block))
          => val indices_list = get_array_indices(block)
          block match {
            case Call("for",List(VarDecl(k,tpv_1,Range(n1_1,n2_1,n3_1)),blk))
              if(indices_list.contains(k) && !indices_list.contains(j))
              => Call("for",List(VarDecl(k,tpv_1,Range(n1_1,n2_1,n3_1)),Call("for",List(VarDecl(j,tpv,Range(n1,n2,n3)),blk))))
            case Block(s)
              => val nf = s.map(get_nested_for_loop(_)).find( x => x.isDefined ).flatten
                nf match {
                  case Some(expr_1)
                   => expr_1 match {
                        case Call("for",List(VarDecl(k,tpv_1,Range(n1_1,n2_1,n3_1)),blk))
                          if(indices_list.contains(k) && !indices_list.contains(j))
                          => val new_blk = remove_for_loop(block)
                          Call("for",List(VarDecl(k,tpv_1,Range(n1_1,n2_1,n3_1)),Call("for",List(VarDecl(j,tpv,Range(n1,n2,n3)),new_blk))))
                        case _ => expr
                      }
                  case None => expr
                }
            case _ => expr
          }
        case Block(s:+Seq(List(Block(Nil))))
          => Block(s.map(reorder_loops(_)))
        case Block(s)
          => Block(s.map(reorder_loops(_)))
        case IfE(p,x,y)
          => IfE(p,reorder_loops(x),reorder_loops(y))
        case _ => expr
      }

    def count_nested_loops ( expr: Expr ): Int
    = expr match {
        case Call("for",List(_,b))
          => count_nested_loops(b)+1
        case _ => 0
      }

    def has_gemm( expr : Expr): Boolean
      = expr match {
          case Call("for",List(_,blk))
            => blk match {
                case Call("for",List(_,blk_1))
                  => blk_1 match {
                      case Block(s)
                        => s.map{
                            case Assign(d,Seq(List(MethodCall(x,m,List(y)))))
                              => y match {
                                  case MethodCall(_,m_1,List(_))
                                    => m_1 == "*" && x == d && m == "+" // Checking for multiply-add pattern
                                  case _ => false
                              }
                            case _ => false
                          }.reduce( (x:Boolean, y:Boolean) => x || y)
                      case _ => false
                    }
                case _ => false
              }
          case _ => false
        }

    e match {
        case Var(v) => v
        case IntConst(n) => n.toString
        case DoubleConst(n) => n.toString+"f"
        case BoolConst(n) => n.toString
        case Nth(x,n)
          => "get<"+(n-1)+">(*"+makeC(x,tabs,false)+")"
        case Index(Var(v),List(n))
          => v+"["+makeC(n,tabs,false)+"]"
        case Index(x,List(n))
          => "(*"+makeC(x,tabs,false)+")["+makeC(n,tabs,false)+"]"
        case MethodCall(x,"++",List(y))
          => "append("+makeC(x,tabs,false)+","+makeC(y,tabs,false)+")"
        case MethodCall(Var("Math"),opr,s)
          => makeC(Call(opr,s))
        case MethodCall(x,op,List(y))
          if binary_oprs.contains(op)
          => "("+makeC(x,tabs,false)+binary_oprs(op)+makeC(y,tabs,false)+")"
        case Call("args",List(x))
          => "argv["+makeC(x,tabs,false)+"+1]"
        case Call("vector",List(n,v))
          => val nc = makeC(n,tabs,false)
             val vc = makeC(v,tabs,false)
             val tc = makeCtype(exprType(v),true)
             "new vector<"+tc+">("+nc+","+vc+")"
        case MethodCall(x,"length",null)
          => makeC(x,tabs,false)+"->size()"
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
              var nb = m.foldLeft[Expr](b){ case (r,(v,u)) => subst(u,Var(v),r) }
              val n1_m = get_arrays(n1)
              val n1_b = n1_m.foldLeft[Expr](n1){ case (r,(v,u)) => subst(u,Var(v),r) }
              val n2_m = get_arrays(n2)
              val n2_b = n2_m.foldLeft[Expr](n2){ case (r,(v,u)) => subst(u,Var(v),r) }
              val n3_m = get_arrays(n3)
              val n3_b = n3_m.foldLeft[Expr](n3){ case (r,(v,u)) => subst(u,Var(v),r) }
              val all_m = m ++ n1_m ++ n2_m ++ n3_m
              var array_set = Set[Expr]()
              val all_m_1 = all_m.filter{
                case (v,u) => {
                  val u_tp = exprType(u)
                  u_tp match {
                    case ArrayType(_,_)
                      if(!array_set.contains(u)) => {
                        array_set += u
                        true
                      }
                    case _ => false
                  }
                }
                case _ => false
              }
              val data = all_m_1.flatMap{
                  case (v,u) => {
                    val u_tp = exprType(u)
                    u_tp match {
                      case ArrayType(_,_) => List(v)
                      case _ => List()
                    }
                  }
                  case _ => List()
                }
              if(use_GPU && has_gemm(b)) {
                all_m_1.flatMap{
                  case (v,u) => {
                    val u_tp = exprType(u)
                    u_tp match {
                      case ArrayType(_,_) => List("auto "+v+" = "+makeC(u,tabs,false)+"->buffer();\n")
                      case _ => List()
                    }
                  }
                  case _ => List()
                }.mkString("")+"\n"+tab(tabs-1)+
                "int device_id = get_gpu_id();\n"+tab(tabs-1)+
                "setDevice(device_id);\n"+tab(tabs-1)+
                writeMLIR(e, data.toList)
              }
              else {
                val loop_text = tab(tabs-1)+"for ( int "+i+" = "+makeC(n1_b,tabs,false)+"; "+i+
                    " <= "+makeC(n2_b,tabs,false)+"; "+i+" += "+makeC(n3_b,tabs,false)+" )\n"
                var device_str = "#pragma acc parallel deviceptr("+data.mkString(",")+")\n#pragma acc loop"
                val loop_count = count_nested_loops(nb)

                val loop_indices = nb match {
                    case Call("for",List(VarDecl(j,tp_1,Range(m1,m2,m3)),blck))
                      => get_vars(m1)++get_vars(m2)++get_vars(m3)
                    case _ => List()
                  }

                if(loop_count >= 1 && !loop_indices.contains(i))
                  device_str += " tile(32,32)"
                else
                  device_str += " tile(1024)"

                if(use_GPU && loop_count > 1 && has_reduction(nb)) {
                  val v = new_var()
                  device_str = tab(tabs)+"float "+v+" = 0.0f;\n"+device_str
                  device_str += " reduction(+:"+v+")"

                  def add_reduction ( expr: Expr ): Expr
                    = expr match {
                      case Call("for",List(p@VarDecl(j,_,_),block))
                        => block match {
                            case Block(s)
                              => var found = false
                                var new_var_assign: Expr = null
                                var var_assign: Expr = null
                                def create_reduction_block(blk: Expr): Expr
                                  = blk match {
                                    case Assign(d,Seq(List(MethodCall(_,m_1,y))))
                                      => val indices_list = get_array_indices(blk)
                                        if(indices_list.contains(j))
                                          blk
                                        else {
                                          val new_blk = Assign(Var(v),Seq(List(MethodCall(Var(v),m_1,y))))
                                          found = true
                                          if(new_var_assign == null)
                                            new_var_assign = Assign(Var(v),makeZero(BasicType("Double")))
                                          if(var_assign == null)
                                            var_assign = Assign(d,Var(v))
                                          new_blk
                                        }
                                    case Block(s)
                                      => Block(s.map(create_reduction_block(_)))
                                    case IfE(p,x,y)
                                      => IfE(p,create_reduction_block(x),create_reduction_block(y))
                                    case _ => blk
                                  }
                                val new_block = create_reduction_block(block)
                                if(found)
                                  Block(List(new_var_assign, Call("for",List(p,new_block)), var_assign))
                                else
                                  Call("for",List(p,block))
                            case _ => Call("for",List(p,add_reduction(block)))
                          }
                      case Block(s:+Seq(List(Block(Nil))))
                        => Block(s.map(add_reduction(_)))
                      case Block(s)
                        => Block(s.map(add_reduction(_)))
                      case IfE(p,x,y)
                        => IfE(p,add_reduction(x),add_reduction(y))
                      case _ => expr
                    }
                  nb = add_reduction(nb)
                }
                device_str += "\n"
                val pragma_str = if(use_GPU) tab(tabs)+"int device_id = get_gpu_id();\n"+tab(tabs)+"setDevice(device_id);\n"+device_str
                  else "#pragma omp parallel for\n"

                "{ "+all_m.flatMap{
                      case (v,u) => {
                        val u_tp = exprType(u)
                        u_tp match {
                          case ArrayType(_,_) => List("auto "+v+" = "+makeC(u,tabs,false)+"->buffer(); ")
                          case _ => List("const auto "+v+" = "+makeC(u,tabs,false)+"; ")
                        }
                      }
                      case _ => List()
                    }.mkString("")+"\n"+
                    pragma_str+
                    loop_text+tab(tabs)+makeC(nb,tabs+1,true)+";\n"+tab(tabs-1)+"}\n"
              }

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
          => val ts = s.map( x => makeCtype(exprType(x),true) ).mkString("<",",",">")
             s.map(makeC(_,tabs,false)).mkString("new tuple"+ts+"(",",",")")
        case Seq(Nil)
          => "nullptr"
        case Seq(List(x))
          => "elem("+makeC(x,tabs,stmt)+")"
        case Seq(s)
          => val tc = makeCtype(exprType(s.head))
             "new vector<"+tc+">({ "+s.map(makeC(_,tabs,false)).mkString(", ")+" })"
        case IfE(p,x,Seq(Nil))
          if false //stmt
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
             makeCtype(tp)+" "+v+" = new vector<"+makeCtype(et,true)+">()"
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
    mlir_writer = new PrintWriter(new File("mlir_output.mlir"))
    val dimx = block_dim_size/gpu_block_x
    mlir_writer.println(s"""#map0 = affine_map<(d0) -> (d0)>
    #map1 = affine_map<(d0) -> (d0 + 128)>
    #map2 = affine_map<(d0) -> (d0 + ${dimx})>
    #map3 = affine_map<(d0,d1) -> (d0 + d1)>
    module {
    memref.global "private" @a_smem_global : memref<${dimx}x128xf32, 3>
    memref.global "private" @b_smem_global : memref<128x${dimx}xf32, 3>
    """)
    writer.println("#include \"runtime.h\"\n")
    if(use_GPU) {
      writer.println("#include \"cuda_util.h\"\n#include <cuda.h>\n")
    }
    val s = makeCxxCode(e)
    val fs = functions.map(f => "functions.push_back((void*(*)(void*))"
                                +makeC(f,0,false)+");\n").mkString("")
    mlir_writer.println("}\n")
    mlir_writer.close()
    writer.println(s"int main ( int argc, char* argv[] ) {\nstartup(argc,argv,$block_dim_size);\n$fs${s}mpi_finalize();\nreturn 0;\n}")
  }
}

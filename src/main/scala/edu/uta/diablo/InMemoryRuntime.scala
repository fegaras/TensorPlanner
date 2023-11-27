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

import Runtime._
import PlanGenerator._


/* Multi-core, in-memory evaluation on a single node without MPI (for testing only) */
object inMem {
  val mstats: Statistics = new Statistics()
  var operations: Array[Opr] = _

  def pe ( e: Any ): Any
    = e match {
        case x: (Any,Any)
          => x._2 match {
               case z: (Any,Any,Array[Double]) @unchecked
                 => z._3.toList
               case z: (Any,Any)
                 => (pe((x._1,z._1)),pe((x._1,z._2)))
               case z => z
             }
        case x => x
      }

  // top-down recursive evaluation
  def eval ( id: OprID, tabs: Int ): Any = {
    val e = operations(id)
    if (hasCachedValue(e)) {
      // retrieve result block(s) from cache
      return e.cached
    }
    info(" "*3*tabs+"*** "+tabs+": "+id+"/"+e)
    val res = e match {
        case LoadOpr(b)
          => Runtime.loadBlocks(b)
        case ApplyOpr(x,fid,args)
          => val f = if (args.isInstanceOf[EmptyTuple] || args==())
                       functions(fid).asInstanceOf[Any=>List[Any]]
                     else functions(fid).asInstanceOf[Any=>Any=>List[Any]](args)
             mstats.apply_operations += 1
             f(eval(x,tabs+1)).head
        case PairOpr(x,y)
          => def f ( lg: OprID ): (Any,Any)
               = operations(lg) match {
                    case PairOpr(x,y)
                      => val gx@(iv,vx) = f(x)
                         val gy@(_,vy) = f(y)
                         val xx = if (operations(x).isInstanceOf[PairOpr]) gx else vx
                         val yy = if (operations(y).isInstanceOf[PairOpr]) gy else vy
                         (iv,(xx,yy))
                    case _ => eval(lg,tabs+1).asInstanceOf[(Any,Any)]
                 }
             f(id)
        case SeqOpr(s)
          => s.map(eval(_,tabs+1))
        case ReduceOpr(s,true,fid)
          => val sv = s.map(eval(_,tabs+1))
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             mstats.reduce_operations += 1
             sv.reduce{ (x:Any,y:Any) => op((x,y)) }
        case ReduceOpr(s,false,fid)
          => val sv = s.map(eval(_,tabs+1).asInstanceOf[(Any,Any)])
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             mstats.reduce_operations += 1
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    info(" "*3*tabs+"*-> "+tabs+": "+res)   // pe(res)
    e.cached = res
    e.status = completed
    for ( x <- children(e) )
      if (operations(x).consumers.forall(c => operations(c).status == completed))
        operations(x).cached = null
    res
  }

  // in-memory evaluation using top-down recursive evaluation on a plan tree
  def evalTopDown[I] ( plan: Plan[I] ): Tensor[I] = {
    val (ds,dd,s) = plan
    operations = PlanGenerator.operations.toArray
    mstats.clear()
    operations.foreach(x => x.count = x.consumers.length)
    val res = s.map{ case (i,lg) => eval(lg,0).asInstanceOf[(I,Any)] }
    info("Number of tasks: "+operations.length)
    (ds,dd,res)
  }

  // in-memory evaluation using TensorPlanner tasks
  def eval[I] ( plan: Plan[I] ): Tensor[I] = {
    val (ds,dd,s) = plan
    exit_points = s.map(_._2)
    info("Exit points: "+exit_points)
    operations = PlanGenerator.operations.toArray
    operations.foreach{ x => x.node = 0 }
    Runtime.operations = operations
    Runtime.loadBlocks = PlanGenerator.loadBlocks.toArray
    for ( x <- operations ) {
      x.status = notReady
      x.cached = null
      initialize_opr(x)
    }
    val entries = entry_points(exit_points)
    info("Entry points: "+entries)
    for ( x <- entries )
       ready_queue.offer(x)
    info("Queue: "+ready_queue)
    while (!ready_queue.isEmpty) {
      val opr_id = ready_queue.poll()
      try {
        compute(opr_id)
      } catch { case ex: Exception
                  => ex.printStackTrace() }
      enqueue_ready_operations(opr_id)
      check_caches(opr_id)
    }
    if (PlanGenerator.trace) {
      info("Number of operations: "+operations.length)
      stats.print()
      val cbs = operations.indices.filter(x => operations(x).cached != null)
      info("Cached blocks: ("+cbs.length+") "+cbs)
    }
    (ds,dd,s.map{ case (i,lg) => (i,operations(lg).cached) })
  }
}

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

import PlanGenerator._
import Scheduler._
import Executor._
import Communication._


@transient
object Runtime {
  var operations: Array[Opr] = _
  var loadBlocks: Array[Any] = _
  var exit_points: List[Int] = Nil

  // depth-first-search: visit each operation only once starting from s
  def DFS ( s: List[OprID], f: OprID => Unit ) {
    def dfs ( c: OprID ) {
      val opr = operations(c)
      if (!opr.visited) {
        opr.visited = true
        f(c)
        opr match {
          case TupleOpr(x,y)
            => dfs(x)
               dfs(y)
          case ApplyOpr(x,_)
            => dfs(x)
          case ReduceOpr(s,_,_)
            => s.foreach(dfs)
          case _ => ;
        }
      }
    }
    for { x <- operations }
      x.visited = false
    s.foreach(dfs)
  }

  def schedule[I,T,S] ( e: Plan[I,T,S] ) {
    if (isMaster()) {
      if (receive_request != null) {
        receive_request.cancel()
        receive_request.free()
        receive_request = null
      }
      operations = PlanGenerator.operations.toArray
      loadBlocks = PlanGenerator.loadBlocks.toArray
      Scheduler.schedule(e)
      broadcast_plan()
      for ( x <- operations ) {
        x.completed = false
        x.cached = null
        // initial count is the number of local consumers
        x.count = x.consumers.count(c => operations(c).node == my_master_rank)
        x match {
          case r@ReduceOpr(s,_,op)   // used for partial reduction
            => r.reduced_count = s.count(c => operations(c).node == my_master_rank)
               if (x.node == my_master_rank)
                 r.reduced_count += s.filter( c => operations(c).node != my_master_rank )
                                     .map(operations(_).node).distinct.length
          case _ => ;
        }
      }
    }
  }

  def cache_data ( opr: Opr, data: Any ): Any = {
    if (opr.cached == null) {
      stats.cached_blocks += 1
      stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
    }
    opr.cached = data
    opr.completed = true
    data
  }

  def compute ( opr_id: OprID ): Any = {
    val opr = operations(opr_id)
    info("*** computing operation "+opr_id+":"+opr+" on node "+my_master_rank)
    val res = opr match {
        case LoadOpr(_,b)
          => cache_data(opr,loadBlocks(b))
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             if (!operations(x).completed)
               error("missing input in Apply: "+opr_id+" at "+my_master_rank)
             stats.apply_operations += 1
             cache_data(opr,f(operations(x).cached).head)
        case TupleOpr(x,y)
          => def f ( lg_id: OprID ): (Any,Any) = {
                val lg = operations(lg_id)
                if (lg.completed) {
                  if (lg.cached == null)
                    error("missing input: "+lg_id+" at "+my_master_rank)
                  lg.cached.asInstanceOf[(Any,Any)]
                } else lg match {
                    case TupleOpr(x,y)
                      => val gx@(iv,vx) = f(x)
                         val gy@(_,vy) = f(y)
                         val xx = if (operations(x).isInstanceOf[TupleOpr]) vx else gx
                         val yy = if (operations(y).isInstanceOf[TupleOpr]) vy else gy
                         val data = (iv,(xx,yy))
                         cache_data(lg,data)
                         data
                    case _ => if (!lg.completed)
                                error("missing input in Load: "+lg_id+" at "+my_master_rank)
                              lg.cached.asInstanceOf[(Any,Any)]
                }
             }
             f(opr_id)
        case ReduceOpr(s,valuep,fid)
          => val sv = s.map{ x => if (!operations(x).completed)
                                    error("missing input in Reduce: "+opr_id+" at "+my_master_rank)
                                  operations(x).cached.asInstanceOf[(Any,Any)] }
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             val in = if (valuep) sv else sv.map(_._2)
             cache_data(opr,(sv.head._1,in.reduce{ (x:Any,y:Any) => op((x,y)) }))
      }
    info("*-> result of operation "+opr_id+":"+opr+" on node "+my_master_rank+" is "+res)
    res
  }

  // garbage-collect the opr block
  def gc ( opr_id: OprID ) {
    val opr = operations(opr_id)
    if (opr.cached != null) {
      info("    discard the cached block of "+opr_id+" at node "+my_master_rank)
      stats.cached_blocks -= 1
      opr.completed = false
      opr.cached = null   // make the cached block available to the garbage collector
    }
  }

  // after opr_id is finished, check if we can release the cache of its children
  def check_caches ( opr_id: OprID ) {
    val e = operations(opr_id)
    for ( c <- children(e) ) {
      val copr = operations(c)
      copr.count -= 1
      if (copr.count <= 0)
        gc(c)
    }
  }

  // After opr is completed, check its consumers to see if anyone is ready to enqueue
  def enqueue_ready_operations ( opr_id: OprID ) {
    val opr = operations(opr_id)
    if (opr.node == my_master_rank) {
      // send the opr cached result to the non-local consumers (must be done first)
      val nodes_to_send_data: List[Int]
          = opr.consumers.map(operations(_)).filter {
                copr => (copr.node != my_master_rank
                         && copr.node >= 0
                         && !copr.isInstanceOf[ReduceOpr])
            }.map(_.node).distinct
      for ( n <- nodes_to_send_data )
        send_data(n,opr.cached,opr_id,0)
    }
    for ( c <- opr.consumers ) {
      val copr = operations(c)
      if (copr.node >= 0)
      copr match {
        case ReduceOpr(s,valuep,fid)  // partial reduce
          => val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             // partial reduce opr inside the copr ReduceOpr
             if (copr.cached == null) {
               stats.cached_blocks += 1
               stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
               copr.cached = opr.cached
             } else if (valuep)
                      copr.cached = op((copr.cached,opr.cached))
               else { val x = copr.cached.asInstanceOf[(Any,Any)]
                      val y = opr.cached.asInstanceOf[(Any,Any)]
                      copr.cached = (x._1,op((x._2,y._2)))
                    }
             copr.reduced_count -= 1
             if (copr.reduced_count == 0) {
               if (copr.node == my_master_rank) {
                 // completed ReduceOpr
                 info("    completed reduce of opr "+opr_id+" at "+my_master_rank)
                 stats.reduce_operations += 1
                 copr.completed = true
                 enqueue_ready_operations(c)
               } else {
                 // completed local reduce => send it to the reduce owner
                 info("    sending partial reduce result of opr "+opr_id+" from "
                      +my_master_rank+" to "+copr.node)
                 send_data(copr.node,copr.cached,c,1)
                 gc(opr_id)
               }
             }
             check_caches(c)
        case _
          => // enqueue the local consumers that are ready
             if (copr.node == my_master_rank
                 && !copr.completed
                 && children(copr).map(operations(_)).forall(_.completed)
                 && !ready_queue.contains(c))
               this.synchronized { ready_queue.offer(c) }
      }
    }
  }

  class ReceiverThread extends Thread {
    override def run () {
      if (receive_request != null) {
        receive_request.cancel()
        receive_request.free()
        receive_request = null
      }
      while (true) {
        check_communication()
        Thread.sleep(10)
      }
    }
  }

  def work () {
    var exit: Boolean = false
    var count: Int = 0
    val receiver = new ReceiverThread()
    receiver.start()
    while (!exit) {
      // check for exit every 100 iterations
      count = (count+1)%100
      if (count == 0)
        exit = exit_poll()
      if (!ready_queue.isEmpty) {
        val opr_id = ready_queue.poll()
        compute(opr_id)
        enqueue_ready_operations(opr_id)
        check_caches(opr_id)
      }
    }
    receiver.stop()
  }

  def entry_points ( s: List[OprID] ): List[OprID] = {
    val b = scala.collection.mutable.ArrayBuffer[OprID]()
    DFS(s,
        c => if (operations(c).isInstanceOf[LoadOpr] && operations(c).node == my_master_rank)
               if (!b.contains(c))
                 b.append(c))
    b.toList
  }

  // distributed evaluation of a scheduled plan using MPI
  def eval[I,T,S] ( plan: Plan[I,T,S] ): Plan[I,T,S]
    = if (isMaster()) {
        exit_points = broadcast_exit_points(plan)
        if (isCoordinator())
          info("Exit points: "+exit_points)
        ready_queue.clear()
        stats.clear()
        for ( x <- entry_points(exit_points) )
          ready_queue.offer(x)
        info("Queue on "+my_master_rank+": "+ready_queue)
        work()
        if (PlanGenerator.trace) {
          val stats = collect_statistics()
          if (isCoordinator()) {
            info("Number of operations: "+operations.length)
            stats.print()
          }
        }
        barrier()
        if (PlanGenerator.trace) {
          val cbs = operations.indices.filter(x => operations(x).cached != null)
          info("Cached blocks at "+my_master_rank+": ("+cbs.length+") "+cbs)
        }
        plan
      } else plan

  // eager evaluation of a single operation
  def evalOpr ( opr_id: OprID ): Any = {
    if (isMaster()) {
      val plan = (1,(),List((0,(1,(),opr_id))))
      operations = PlanGenerator.operations.toArray
      loadBlocks = PlanGenerator.loadBlocks.toArray
      schedule(plan)
      eval(plan)
      cache_data(operations(opr_id),broadcast(opr_id,operations(opr_id).cached))
      operations(opr_id).cached.asInstanceOf[(Any,Any)]._2
    } else null
  }

  // collect the results of an evaluated plan at the coordinator
  def collect[I,T,S] ( plan: Plan[I,T,S] ): List[(I,Any)]
    = if (isMaster()) {
        var receiver: ReceiverThread = null
        if (isCoordinator()) {
          receiver = new ReceiverThread()
          receiver.start()
        } else exit_points.foreach {
            opr_id => val opr = operations(opr_id)
                      if (opr.node == my_master_rank)
                        send_data(0,opr.cached,opr_id,2)
          }
        if (isCoordinator()) {
          while (exit_points.exists{ opr_id => !operations(opr_id).completed })
              Thread.sleep(100)
          receiver.stop()
        }
        barrier()
        if (isCoordinator())
          exit_points.map(x => operations(x).cached.asInstanceOf[(I,Any)])
        else Nil
      } else Nil
}


/****************************************************************************************************
* 
* Single-core, in-memory evaluation (for testing only)
* 
****************************************************************************************************/

object inMem {
  val stats: Statistics = new Statistics()
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

  def eval ( id: OprID, tabs: Int ): Any = {
    val e = operations(id)
    if (e.completed) {
      // retrieve result block(s) from cache
      return e.cached
    }
    info(" "*3*tabs+"*** "+tabs+": "+id+"/"+e)
    val res = e match {
        case LoadOpr(_,b)
          => loadBlocks(b)
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             stats.apply_operations += 1
             f(eval(x,tabs+1)).head
        case TupleOpr(x,y)
          => def f ( lg: OprID ): (Any,Any)
               = operations(lg) match {
                    case TupleOpr(x,y)
                      => val gx@(iv,vx) = f(x)
                         val gy@(_,vy) = f(y)
                         val xx = if (operations(x).isInstanceOf[TupleOpr]) vx else gx
                         val yy = if (operations(y).isInstanceOf[TupleOpr]) vy else gy
                         (iv,(xx,yy))
                    case _ => eval(lg,tabs+1).asInstanceOf[(Any,Any)]
                 }
             f(id)
        case ReduceOpr(s,true,fid)
          => val sv = s.map(eval(_,tabs+1))
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             sv.reduce{ (x:Any,y:Any) => op((x,y)) }
        case ReduceOpr(s,false,fid)
          => val sv = s.map(eval(_,tabs+1).asInstanceOf[(Any,Any)])
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    info(" "*3*tabs+"*-> "+tabs+": "+res)   // pe(res)
    e.cached = res
    e.completed = true
    res
  }

  def eval[I,T,S] ( e: Plan[I,T,S] ): (T,S,List[(I,Any)])
    = e match {
        case (dp,sp,s)
          => operations = PlanGenerator.operations.toArray
             stats.clear()
             operations.foreach(x => x.count = x.consumers.length)
             val res = s.map{ case (i,(ds,ss,lg))
                                => eval(lg,0).asInstanceOf[(I,Any)] }
             info("Number of nodes: "+operations.length)
             (dp,sp,res)
      }
}

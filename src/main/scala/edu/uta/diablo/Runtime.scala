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

import Scheduler._
import Executor._
import Communication._
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec


object Runtime {
  val trace = true
  var operations: Array[Opr] = _
  var loadBlocks: Array[Any] = _
  var exit_points: List[Int] = Nil

  // visit each operation only once
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
          case ReduceOpr(s,_)
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
    if (isCoordinator()) {
      Scheduler.schedule(e)
      operations = Scheduler.operations.toArray
      loadBlocks = Scheduler.loadBlocks.toArray
    }
    if (isMaster()) {
      broadcast_plan()
      // initial count is the number of local consumers
      for ( x <- operations )
        x.count = x.consumers.filter( c => operations(c).node == my_master_rank ).length
    }
  }

  def cache_data ( opr: Opr, data: Any ): Any = {
    stats.cached_blocks += 1
    stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
    opr.cached = data
    data
  }

  def compute ( opr_id: OprID ): Any = {
    val opr = operations(opr_id)
    if (trace)
      println("*** computing operation "+opr_id+":"+opr+" on node "+my_master_rank)
    val res = opr match {
        case LoadOpr(_,b)
          => cache_data(opr,loadBlocks(b))
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             assert(operations(x).cached != null)
             stats.apply_operations += 1
             cache_data(opr,f(operations(x).cached).head)
        case opr@TupleOpr(x,y)
          => def f ( lg: Opr ): (Any,Any)
               = if (lg.cached != null)
                   lg.cached.asInstanceOf[(Any,Any)]
                 else lg match {
                    case TupleOpr(x,y)
                      => val gx@(iv,vx) = f(operations(x))
                         val gy@(_,vy) = f(operations(y))
                         val xx = if (operations(x).isInstanceOf[TupleOpr]) vx else gx
                         val yy = if (operations(y).isInstanceOf[TupleOpr]) vy else gy
                         val data = (iv,(xx,yy))
                         cache_data(lg,data)
                         data
                    case _ => assert(lg.cached != null)
                              lg.cached.asInstanceOf[(Any,Any)]
                 }
             f(opr)
        case ReduceOpr(s,fid)
          => val sv = s.map{ x => assert(operations(x).cached != null)
                                  operations(x).cached.asInstanceOf[(Any,Any)] }
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             cache_data(opr,(sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) }))
      }
    if (trace)
      println("*-> result of operation "+opr_id+":"+opr+" on node "+my_master_rank+" is "+res)
    res
  }

  def enqueue_ready_operations ( opr_id: OprID ) {
    val opr = operations(opr_id)
    var nodes_to_send_data: List[Int] = Nil
    for ( c <- opr.consumers ) {
      val copr = operations(c)
      if (opr.node == my_master_rank && copr.node != my_master_rank) {
        if (!nodes_to_send_data.contains(copr.node))
          nodes_to_send_data = copr.node::nodes_to_send_data
      } else if (copr.node == my_master_rank
                 && copr.cached == null
                 && children(copr).map(operations(_)).forall(_.cached != null)
                 && !ready_queue.contains(c))
               this.synchronized { ready_queue.offer(c) }
    }
    for ( n <- nodes_to_send_data )
      send_data(n,opr.cached,opr_id)
  }

  // after the operation is computed, check if we can release the cache of its children
  def check_caches ( opr_id: OprID ) {
    val e = operations(opr_id)
    for ( c <- children(e) ) {
      val copr = operations(c)
      if (copr.node == e.node) {
        copr.count -= 1
        if (copr.count == 0) {
          if (false && trace)
            println("    discard the cached block of "+c+" at node "+my_master_rank)
          stats.cached_blocks -= 1
          copr.cached = null   // garbage-collect the c block
        }
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
      count = (count+1)%100   // check for exit every 100 iterations
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
    val b = ArrayBuffer[OprID]()
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
        if (trace && isCoordinator())
          println("Exit points: "+exit_points)
        ready_queue.clear()
        for ( x <- entry_points(exit_points) )
          ready_queue.offer(x)
        if (trace)
          println("Queue on "+my_master_rank+": "+ready_queue)
        work()
        if (trace) {
          val stats = collect_statistics()
          if (isCoordinator()) {
            println("Number of operations: "+operations.length)
            stats.print()
          }
        }
        barrier()
        plan
      } else plan

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
                        send_data(0,opr.cached,opr_id)
          }
        if (isCoordinator()) {
          while (exit_points.filter {
                        opr_id => operations(opr_id).cached == null }.length > 0)
              {}
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
    if (e.cached != null) {
      // retrieve result block(s) from cache
      val res = e.cached
      e.count -= 1
      if (e.count <= 0) {
        if (trace)
          println(" "*tabs*3+"* "+"discard the cached value of "+id)
        stats.cached_blocks -= 1
        e.cached = null
      }
      return res
    }
    if (trace)
      println(" "*3*tabs+"*** "+tabs+": "+id+"/"+e)
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
        case ReduceOpr(s,fid)
          => val sv = s.map(eval(_,tabs+1).asInstanceOf[(Any,Any)])
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    if (trace)
      println(" "*3*tabs+"*-> "+tabs+": "+res)   // pe(res)
    e.count -= 1
    if (e.count > 0) {
      e.cached = res
      if (trace)
        println(" "*tabs*3+"* "+"cache the value of "+id)
      stats.cached_blocks += 1
      stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
    }
    res
  }

  def eval[I,T,S] ( e: Plan[I,T,S] ): (T,S,List[(I,Any)])
    = e match {
        case (dp,sp,s)
          => operations = Scheduler.operations.toArray
             stats.clear()
             operations.foreach(x => x.count = x.consumers.length)
             val res = s.map{ case (i,(ds,ss,lg))
                                => eval(lg,0).asInstanceOf[(I,Any)] }
             println("Number of nodes: "+operations.length)
             stats.print()
             (dp,sp,res)
      }
}

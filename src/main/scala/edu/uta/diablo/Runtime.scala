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
import Communication.barrier
import scala.collection.mutable.{Queue,ArrayBuffer}
import java.util.Comparator
import java.util.concurrent.{LinkedBlockingQueue,PriorityBlockingQueue}
import mpi.MPI.wtime


@transient
class Statistics (
      var apply_operations: Int = 0,
      var reduce_operations: Int = 0,
      var cached_blocks: Int = 0,
      var max_cached_blocks: Int = 0 ) {

  def clear () {
    apply_operations = 0
    reduce_operations = 0
    cached_blocks = 0
    max_cached_blocks = 0
  }

  def print () {
    info("Number of apply operations: "+apply_operations)
    info("Number of reduce operations: "+reduce_operations)
    info("Max num of cached blocks per executor: "+max_cached_blocks)
    //info("Final num of cached blocks: "+cached_blocks)
  }
}

@transient
object operator_comparator extends Comparator[OprID] {
  def priority ( opr_id: OprID ): Int
    = operations(opr_id) match {
        case ReduceOpr(_,_,_) => 1
        case PairOpr(_,_) => 2
        case LoadOpr(_) => 3
        case _ => 4
      }
  override def compare ( x: OprID, y: OprID ): Int
    = priority(x) - priority(y)
}

@transient
object Runtime {
  var operations: Array[Opr] = _
  var loadBlocks: Array[Any] = _
  var exit_points: List[OprID] = Nil
  // operations ready to be executed
  var ready_queue = new PriorityBlockingQueue[OprID](1000,operator_comparator)
  var send_queue = new LinkedBlockingQueue[(List[WorkerID],OprID)](100)
  var stats: Statistics = new Statistics()
  var enable_partial_reduce = true
  var enable_gc: Boolean = true

  // breadth-first-search: visit each operation only once starting from s
  def BFS ( s: List[OprID], f: OprID => Unit ) {
    for { x <- operations }
      x.visited = false
    var opr_queue: Queue[OprID] = Queue()
    s.foreach(c => opr_queue.enqueue(c))
    while(opr_queue.nonEmpty) {
      val c = opr_queue.dequeue
      val opr = operations(c)
      if (!opr.visited) {
        opr.visited = true
        f(c)
        opr match {
          case PairOpr(x,y)
            => opr_queue.enqueue(x)
               opr_queue.enqueue(y)
          case ApplyOpr(x,_)
            => opr_queue.enqueue(x)
          case ReduceOpr(s_list,_,_)
            => s_list.foreach(op => opr_queue.enqueue(op))
          case SeqOpr(s_list)
            => s_list.foreach(op => opr_queue.enqueue(op))
          case _ => ;
        }
      }
    }
  }

  def hasCachedValue ( x: Opr ): Boolean
    = (x.status == computed || x.status == completed
       || x.status == locked || x.status == zombie)

  def initialize_opr ( x: Opr ) {
    // a Load opr is cached in the coordinator too
    if (isCoordinator() && x.isInstanceOf[LoadOpr])
      cache_data(x,loadBlocks(x.asInstanceOf[LoadOpr].block))
    // initial count is the number of local consumers
    x.count = x.consumers.count(c => operations(c).node == executor_rank)
    x match {
      case r@ReduceOpr(s,_,op)   // used for partial reduction
        => r.reduced_count = s.count(c => operations(c).node == executor_rank)
           if (x.node == executor_rank)
             r.reduced_count += s.filter( c => operations(c).node != executor_rank )
                                 .map(operations(_).node).distinct.length
      case _ => ;
    }
  }

  // used for garbage collection with recovery
  def set_closest_local_descendants ( opr_id: OprID ) {
    val opr_queue: Queue[OprID] = Queue()
    opr_queue.enqueue(opr_id)
    val buffer = ArrayBuffer[OprID]()
    val node = operations(opr_id).node
    while (opr_queue.nonEmpty) {
      val c = opr_queue.dequeue
      val opr = operations(c)
      if (!buffer.contains(c))
        if (opr.node == node) {
          buffer.append(c)
          opr.oc += 1
        } else children(opr).foreach(x => opr_queue.enqueue(x))
    }
    operations(opr_id).os = buffer.toList
  }

  def schedule[I] ( e: Plan[I] ) {
    if (receive_request != null) {
      receive_request.cancel()
      receive_request.free()
      receive_request = null
    }
    operations = PlanGenerator.operations.toArray
    loadBlocks = PlanGenerator.loadBlocks.toArray
    val t = System.currentTimeMillis()
    Scheduler.schedule(e)
    if (isCoordinator())
      info("Scheduling time: %.5f secs".format((System.currentTimeMillis()-t)/1000.0))
    val time = wtime()
    broadcast_plan()
    for ( x <- operations ) {
      x.status = notReady
      x.cached = null
      initialize_opr(x)
    }
    for ( opr_id <- operations.indices )
      set_closest_local_descendants(opr_id)
    if (isCoordinator())
      info("Setup time: %.3f secs".format((wtime-time)/1000.0))
  }

  def cache_data ( opr: Opr, data: Any ): Any = {
    if (opr.cached == null) {
      stats.cached_blocks += 1
      stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
    }
    opr.cached = data
    data
  }

  def compute ( opr_id: OprID ): Any = {
    val opr = operations(opr_id)
    info("*** computing opr "+opr_id+":"+opr+" on exec "+executor_rank)
    val res = opr match {
        case LoadOpr(b)
          => opr.status = computed
             cache_data(opr,loadBlocks(b))
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             if (!hasCachedValue(operations(x)))
               error("missing input in Apply: "+opr_id+" at "+executor_rank)
             stats.apply_operations += 1
             opr.status = computed
             cache_data(opr,f(operations(x).cached).head)
        case PairOpr(x,y)
          => def f ( lg_id: OprID ): (Any,Any) = {
                val lg = operations(lg_id)
                if (hasCachedValue(lg)) {
                  assert(lg.cached != null)
                  lg.cached.asInstanceOf[(Any,Any)]
                } else lg match {
                    case PairOpr(x,y)
                      => val gx@(iv,vx) = f(x)
                         val gy@(_,vy) = f(y)
                         val xx = if (operations(x).isInstanceOf[PairOpr])
                                    gx else vx
                         val yy = if (operations(y).isInstanceOf[PairOpr])
                                    gy else vy
                         val data = (iv,(xx,yy))
                         cache_data(lg,data)
                         lg.status = computed
                         data
                    case _ => assert(hasCachedValue(lg))
                              lg.cached.asInstanceOf[(Any,Any)]
                }
             }
             f(opr_id)
        case SeqOpr(s)
          => val sv = s.map{ x => assert(hasCachedValue(operations(x)))
                                  operations(x).cached }
             opr.status = computed
             cache_data(opr,sv)
        case ReduceOpr(s,valuep,fid)
          => val sv = s.map{ x => assert(hasCachedValue(operations(x)))
                                  operations(x).cached.asInstanceOf[(Any,Any)] }
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             stats.reduce_operations += 1
             val in = if (valuep) sv else sv.map(_._2)
             opr.status = computed
             cache_data(opr,(sv.head._1,in.reduce{ (x:Any,y:Any) => op((x,y)) }))
      }
    info("*-> result of opr "+opr_id+":"+opr+" on exec "+executor_rank+" is "+res)
    res
  }

  // garbage-collect the opr block
  def gc ( opr_id: OprID ) {
    val opr = operations(opr_id)
    if (enable_gc && opr.cached != null) {
      if (opr.status == locked)
        opr.status = zombie
      else {
          info("    discard the cached block of "
               +opr_id+" at exec "+executor_rank)
          stats.cached_blocks -= 1
          opr.status = removed
          opr.cached = null   // make the cached block available to the garbage collector
      }
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

  def enqueue_reduce_opr ( opr_id: OprID, c: OprID ) {
    val opr = operations(opr_id)
    val copr = operations(c)
    copr match {
      case ReduceOpr(s,valuep,fid)  // partial reduce
        if enable_partial_reduce
        => val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
           // partial reduce opr inside the copr ReduceOpr
           if (copr.cached == null) {
             stats.cached_blocks += 1
             stats.max_cached_blocks = Math.max(stats.max_cached_blocks,
                                                stats.cached_blocks)
             copr.cached = opr.cached
           } else if (valuep)
                    // total aggregation
                    copr.cached = op((copr.cached,opr.cached))
             else { val x = copr.cached.asInstanceOf[(Any,Any)]
                    val y = opr.cached.asInstanceOf[(Any,Any)]
                    // merge the current state with the partially reduced data
                    copr.cached = (x._1,op((x._2,y._2)))
                  }
           copr.reduced_count -= 1
           if (copr.reduced_count == 0) {
             if (copr.node == executor_rank) {
               // completed ReduceOpr
               info("    completed reduce of opr "+c+" at "+executor_rank)
               stats.reduce_operations += 1
               copr.status = computed
               enqueue_ready_operations(c)
             } else {
               // completed local reduce => send it to the reduce owner
               info("    sending partial reduce result of opr "+c+" from "
                    +executor_rank+" to "+copr.node)
               send_data(List(copr.node),copr.cached,c,1)
               gc(c)
             }
           }
           // copr is done with opr => check if we can GC opr
           opr.count -= 1
           if (opr.count <= 0)
             gc(opr_id)
      case _ => ;
    }
  }

  // After opr is computed, check its consumers to see if anyone is ready to enqueue
  def enqueue_ready_operations ( opr_id: OprID ) {
    val opr = operations(opr_id)
    if (opr.node == executor_rank) {
      // send the opr cached result to the non-local consumers (must be done first)
      val nodes_to_send_data
          = opr.consumers.map(operations(_)).filter {
                copr => (copr.node != executor_rank
                         && copr.node >= 0
                         && (!enable_partial_reduce
                             || !copr.isInstanceOf[ReduceOpr]))
            }.map(_.node).distinct
      if (nodes_to_send_data.nonEmpty) {
        opr.status = locked
        send_queue.offer((nodes_to_send_data,opr_id))
      }
    }
    for ( c <- opr.consumers ) {
      val copr = operations(c)
      if (copr.node >= 0)
      copr match {
        case ReduceOpr(s,valuep,fid)  // partial reduce
          if enable_partial_reduce
          => enqueue_reduce_opr(opr_id,c)
        case _
          => // enqueue the local consumers that are ready
             if (copr.node == executor_rank
                 && !hasCachedValue(copr)
                 && children(copr).map(operations(_)).forall(hasCachedValue)
                 && !ready_queue.contains(c))
               this.synchronized { ready_queue.offer(c) }
      }
    }
    // if there is no consumer on the same node, garbage collect the task cache
    if (opr.node == executor_rank && !exit_points.contains(opr_id)
        && opr.consumers.forall( c => operations(c).node != executor_rank ))
      gc(opr_id)
    if (opr.status != locked)
      opr.status = completed
  }

  var receiver: ReceiverThread = _
  var sender: SenderThread = _
  var skip_work = false

  var abort_count = 0
  val steps_before_abort = 2
  // kill one of the executors to test recovery from a fault
  def kill_executor ( executor: WorkerID ) {
    abort_count += 1
    if (enable_recovery && executor_rank == executor
        && abort_count == steps_before_abort) {
      info("Killing executor "+executor+" to test recovery")
      if (!accumulator.exit)  // abort exit_poll
        send_data(List(coordinator),false,5)
      skip_work = true
      receiver.stop()
      sender.stop()
      while (true)
        Thread.sleep(1000000)
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
        Thread.sleep(1)
      }
    }
  }

  class SenderThread extends Thread {
    override def run () {
      while (true) {
        if (!send_queue.isEmpty) {
          val (dest,opr_id) = send_queue.poll()
          val opr = operations(opr_id)
          send_data(dest,opr.cached,opr_id,0)
          if (opr.status == zombie) {
            info("    delayed discard the cached block of "
                 +opr_id+" at exec "+executor_rank)
            stats.cached_blocks -= 1
            opr.status = removed
            opr.cached = null     // allow gc to reclaim it
          }
        } else Thread.sleep(1)
      }
    }
  }

  def work () {
    var exit: Boolean = false
    var count: Int = 0
    while (!exit) {
      kill_executor(1)  // kill executor 1 to test recovery
      // check for exit every 100 iterations
      count = (count+1)%100
      if (count == 0)
        exit = exit_poll()
      if (!ready_queue.isEmpty && !skip_work) {
          val opr_id = ready_queue.poll()
          compute(opr_id)
          enqueue_ready_operations(opr_id)
          check_caches(opr_id)
      }
    }
    info("Executor "+executor_rank+" has finished execution")
  }

  def entry_points ( es: List[OprID] ): List[OprID] = {
    val b = ArrayBuffer[OprID]()
    BFS(es,
        c => { val copr = operations(c)
               if (copr.isInstanceOf[LoadOpr] && copr.node == executor_rank)
                 if (!b.contains(c))
                   b.append(c) })
    b.toList
  }

  // distributed evaluation of a scheduled plan using MPI
  def eval[I] ( plan: Plan[I] ): Plan[I] = {
    val time = wtime()
    exit_points = plan._3.map(_._2)
    exit_points = broadcast_exit_points(exit_points)
    if (isCoordinator())
      info("Exit points: "+exit_points)
    ready_queue.clear()
    stats.clear()
    for ( x <- entry_points(exit_points) )
       ready_queue.offer(x)
    info("Queue on "+executor_rank+": "+ready_queue)
    receiver = new ReceiverThread()
    receiver.start()
    sender = new SenderThread()
    sender.start()
    work()
    if (PlanGenerator.trace) {
      val stats = collect_statistics()
      if (isCoordinator()) {
        info("Number of operations: "+operations.length)
        stats.print()
      }
    }
    barrier()
    receiver.stop()
    sender.stop()
    if (PlanGenerator.trace) {
      info("Evaluation time: %.3f secs".format((wtime()-time)/1000.0))
      val cbs = operations.indices.filter(x => operations(x).cached != null)
      info("Cached blocks at "+executor_rank+": ("+cbs.length+") "+cbs)
    }
    plan
  }

  // eager evaluation of a single operation
  def evalOpr ( opr_id: OprID ): Any = {
    val plan = (1,EmptyTuple(),List((0,opr_id)))
    operations = PlanGenerator.operations.toArray
    loadBlocks = PlanGenerator.loadBlocks.toArray
    schedule(plan)
    eval(plan)
    val opr = operations(opr_id)
    opr.status = computed
    cache_data(opr,broadcast(opr_id,operations(opr_id).cached))
    opr.cached.asInstanceOf[(Any,Any)]._2
  }

  // collect the results of an evaluated plan at the coordinator
  def collect[I] ( plan: Plan[I] ): Tensor[I] = {
    val (ds,dd,es) = plan
    var receiver: ReceiverThread = new ReceiverThread()
    receiver.start()
    if (isCoordinator()) {
        info("Collecting the blocks of tasks "+es+" at the coordinator")
        while (es.exists{ case (_,opr_id) => !hasCachedValue(operations(opr_id)) })
          Thread.sleep(100)
        barrier()
        receiver.stop()
        (ds,dd,es.map{ case (i,opr_id) => (i,operations(opr_id).cached) })
    } else {
        es.foreach {
             case (i,opr_id)
               => val opr = operations(opr_id)
                  if (opr.node == executor_rank)
                    send_data(List(coordinator),opr.cached,opr_id,2)
        }
        barrier()
        receiver.stop()
        (ds,dd,Nil)
    }
  }

  def completed_front ( failed_executor: WorkerID, new_executor: WorkerID ): Array[OprID] = {
    for { x <- operations }
      x.visited = false
    val opr_queue: Queue[OprID] = Queue()
    val buffer = ArrayBuffer[OprID]()
    exit_points.foreach(c => opr_queue.enqueue(c))
    while (opr_queue.nonEmpty) {
      val c = opr_queue.dequeue
      val opr = operations(c)
      if (!opr.visited) {
        opr.visited = true
        opr match {
          case r@ReduceOpr(s,_,_)   // used for partial reduction
            => def nn ( c: OprID ): OprID
                 = if (operations(c).node == failed_executor)
                     new_executor
                   else operations(c).node
               r.cached = null
               r.reduced_count = s.count(nn(_) == executor_rank)
               if (nn(c) == executor_rank)
                 r.reduced_count += s.filter(nn(_) != executor_rank)
                                     .map(nn(_)).distinct.length
          case _ => ;
        }
        if (opr.node == executor_rank && hasCachedValue(opr))
          buffer.append(c)
        else if (executor_rank == 0 && opr.isInstanceOf[LoadOpr])
          buffer.append(c)
        else children(opr).foreach(opr_queue.enqueue(_))
      }
    }
    buffer.toArray
  }

  // recovery from failure
  def recover ( failed_executor: WorkerID, new_executor: WorkerID ) {
    skip_work = true
    accumulator.reset()
    info("Executor "+executor_rank+": Recovering from the failed executor by replacing "
         +failed_executor+" with "+new_executor)
    val front = completed_front(failed_executor,new_executor)
    info("Executor "+executor_rank+": completed front = "+front.toList)
    for ( opr_id <- front;
          c <- operations(opr_id).consumers
          if operations(c).isInstanceOf[ReduceOpr] )
       enqueue_reduce_opr(opr_id,c)
    if (executor_rank != new_executor) {
      for ( opr_id <- front ) {
        val opr = operations(opr_id)
        if ((opr.consumers.exists(c => operations(c).node == failed_executor)
             && (opr.status != completed
                 || !opr.consumers.exists(c => operations(c).node == new_executor)))
            || (opr.isInstanceOf[LoadOpr] && executor_rank == 0))
          send_data(List(new_executor),opr.cached,opr_id,0)
      }
    }
    for ( opr_id <- operations.indices ) {
      val opr = operations(opr_id)
      if (opr.node == failed_executor)
        opr.node = new_executor
    }
    info("Executor "+executor_rank+": Recovered from the failed executor "+failed_executor)
    skip_work = false
  }
}

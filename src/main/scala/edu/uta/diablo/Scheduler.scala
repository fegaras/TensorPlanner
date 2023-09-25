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
import scala.collection.mutable.{ListBuffer, Queue}


object Scheduler {
  var max_lineage_length = 3
  // work done at each processor
  var work: Array[Int] = _
  // # of tasks at each processor
  var tasks: Array[Int] = _
  // the pool of ready nodes
  var ready_pool: ListBuffer[OprID] = _

  // number of blocks returned by operation e
  def size ( e: Opr ): Int = {
    if (e.size <= 0)
      e.size = e match {
                  case TupleOpr(x,y)
                    => size(operations(x)) + size(operations(y))
                  case _ => 1
               }
    e.size
  }

  def set_sizes () {
    for ( x <- operations )
      size(x)
  }

  def set_blevel[I,T,S] ( e: Plan[I,T,S] ) {
    var opr_queue: Queue[(Opr,Int)] = Queue()
    val exit_points = e._3.map(x => x._2._3)
    exit_points.foreach{ c => opr_queue.enqueue((operations(c),0)) }
    while(opr_queue.nonEmpty) {
      val (cur_opr, cur_blevel) = opr_queue.dequeue
      if (cur_blevel > cur_opr.static_blevel) {
        cur_opr.static_blevel = cur_blevel
        children(cur_opr).foreach{ c => opr_queue.enqueue((operations(c),cur_blevel+1)) }
      }
    }
    for ( x <- operations )
      if (x.isInstanceOf[TupleOpr])
        x.static_blevel += size(x)
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

  def entry_points ( s: List[OprID] ): List[OprID] = {
    val b = scala.collection.mutable.ArrayBuffer[OprID]()
    Runtime.BFS(s,
        c => if (operations(c).isInstanceOf[LoadOpr])
               if (!b.contains(c))
                 b.append(c))
    b.toList
  }

  import scala.collection.mutable.PriorityQueue
  case class Worker ( workDone: Int, numTasks: Int, id: WorkerID )

  object MinWorkerOrder extends Ordering[Worker] {
    def compare(x:Worker, y:Worker) = {
      if(x.workDone == y.workDone)
        y.numTasks compare x.numTasks
      else 
        y.workDone compare x.workDone
    }
  }

  // priority queue of work done at each processor
  var workerQueue: PriorityQueue[Worker] = PriorityQueue.empty(MinWorkerOrder)

  def schedule[I,T,S] ( e: Plan[I,T,S] ) {
    set_sizes()
    set_blevel(e)
    var task_queue: Queue[OprID] = Queue()
    work = 0.until(Communication.num_of_executors).map(w => 0).toArray
    tasks = 0.until(Communication.num_of_executors).map(w => 0).toArray
    var in_degree: Array[Int] = Array.ofDim[Int](operations.size)
    for(op <- operations) {
      for(c <- op.consumers) {
        in_degree(c) += 1
      }
    }
    var ep: ListBuffer[OprID] = ListBuffer[OprID]()
    for(i <- 0 until in_degree.size) {
      if(in_degree(i) == 0) ep += i
    }
    if (isCoordinator()) {
      println("Size: "+ep.size+" Entry points: "+ep)
    }

    for (op <- ep) {
      task_queue.enqueue(op)
    }
    for(i <- 0 until Communication.num_of_executors)
      workerQueue.enqueue(Worker(0,0,i))

    var total_time = 0.0
    var worker_time = 0.0
    val k = scala.math.sqrt(Communication.num_of_executors).toInt+1
    while (task_queue.nonEmpty) {
      val c = task_queue.dequeue
      val opr = operations(c)
      var tmp_workers: ListBuffer[Worker] = ListBuffer()
      var w = Worker(0,0,0)
      var min_comm_cost = Int.MaxValue
      for(i <- 0 until k) {
        var tmp_worker = workerQueue.dequeue()
        tmp_workers += tmp_worker
        val tmp_cost = communication_cost(opr,tmp_worker.id)
        if(tmp_cost < min_comm_cost) {
          w = tmp_worker
          min_comm_cost = tmp_cost
        }
      }
      for(tw <- tmp_workers) {
        if(tw.id != w.id) workerQueue.enqueue(tw)
      }
      // opr is allocated to worker w
      opr match {
        case ReduceOpr(_,true,_)  // total aggregation
          => opr.node = 0
        case _ => opr.node = w.id
      }
      val work_done = cpu_cost(opr) + communication_cost(opr,w.id)
      workerQueue.enqueue(Worker(w.workDone + work_done, w.numTasks+1, w.id))
      work(w.id) += work_done
      tasks(w.id) += 1
      if (isCoordinator())
        info("schedule opr "+c+" on node "+w.id+" (work = "+work(w.id)+")")
      // add more ready nodes
      for { c <- opr.consumers } {
        val copr = operations(c)
        in_degree(c) -= 1
        if (in_degree(c) == 0) {
          task_queue.enqueue(c)
        }
      }
    }
    if (trace && isCoordinator())
      print_plan(e)
  }
}

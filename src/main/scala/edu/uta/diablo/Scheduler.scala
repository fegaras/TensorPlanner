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

import PlanGenerator._
import scala.collection.mutable.{ListBuffer, Queue}
import scala.collection.mutable.PriorityQueue


object Scheduler {
  val trace = false
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
                  case PairOpr(x,y)
                    => size(operations(x)) + size(operations(y))
                  case _ => 1
               }
    e.size
  }

  def set_sizes () {
    for ( x <- operations )
      size(x)
  }

  def set_blevel[I] ( e: Plan[I] ) {
    var opr_queue: Queue[(Opr,Int)] = Queue()
    val exit_points = e._3.map(x => x._2)
    exit_points.foreach{ c => opr_queue.enqueue((operations(c),0)) }
    while(opr_queue.nonEmpty) {
      val (cur_opr, cur_blevel) = opr_queue.dequeue
      if (cur_blevel > cur_opr.static_blevel) {
        cur_opr.static_blevel = cur_blevel
        children(cur_opr).foreach{ c => opr_queue.enqueue((operations(c),cur_blevel+1)) }
      }
    }
    for ( x <- operations )
      if (x.isInstanceOf[PairOpr])
        x.static_blevel += size(x)
  }

  def cpu_cost ( opr: Opr ): Int = opr.cpu_cost

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

  def get_worker_node( coord: Any, n: Int, m: Int) = {
    val num_exec = Communication.num_of_executors
    val row_cnt = (Math.sqrt(num_exec)).toInt
    val col_cnt = (num_exec/row_cnt).toInt
    val row_sz = (Math.ceil(n.toDouble/row_cnt)).toInt
    val row_sz1 = (Math.ceil(n.toDouble/Math.min(n,num_exec))).toInt
    val col_sz = (Math.ceil(m.toDouble/col_cnt)).toInt
    coord match {
      case i:Int => (i/row_sz1)
      case (i:Int,j:Int) => (i/row_sz)*col_cnt + j/col_sz
      case _ => Math.abs(coord.##) % num_exec
    }
  }

  def schedule[I] ( e: Plan[I]) {
    set_sizes()
    set_blevel(e)
    var taskQueue: Queue[OprID] = Queue()
    var in_degree: Array[Int] = Array.ofDim[Int](operations.size)
    for(op <- operations) {
      for(c <- op.consumers) {
        in_degree(c) += 1
      }
    }
    var entry_p: ListBuffer[OprID] = ListBuffer[OprID]()
    for(i <- 0 until in_degree.size) {
      if(in_degree(i) == 0)
        entry_p += i
    }
    if (isCoordinator()) {
      println("Entry points: size:["+entry_p.length+"]: "+entry_p)
    }
    var n = entry_p.map{case ep => {
      operations(ep).coord match {
        case i: Int => i
        case (i:Int,j:Int) => i
        case (i:Int,j:Int,k:Int) => i
      }
    }}.max+1
    var m = entry_p.map{case ep => {
      operations(ep).coord match {
        case i: Int => i
        case (i:Int,j:Int) => j
        case (i:Int,j:Int,k:Int) => j
      }
    }}.max+1
    for ( opr_id <- operations.indices) {
      val opr = operations(opr_id)
      opr match {
        case PairOpr(_,_)
          => val p_opr = operations(opr.consumers(0))
            val w = opr.coord match {
              case i:Int
                => i%Communication.num_of_executors
              case _ => Math.abs(opr.coord.##)%Communication.num_of_executors
            }
            if(p_opr.consumers.length > 0) {
              val gp_opr = operations(p_opr.consumers(0))
              opr.node = gp_opr match {
                case ReduceOpr(_,false,_)
                  => get_worker_node(p_opr.coord,n,m)
                case _ => w
              }
            }
            else opr.node = w
            if(opr.node != -1) taskQueue.enqueue(opr_id)
        case LoadOpr(_)
         => taskQueue.enqueue(opr_id)
         opr.node = get_worker_node(opr.coord,n,m)
        case _ => ;
      }
    }
    while (taskQueue.nonEmpty) {
      val cur_task = taskQueue.dequeue()
      val opr = operations(cur_task)
      // add more ready nodes
      for { op <- (opr.consumers ++ children(opr)).distinct } {
        var copr =  operations(op)
        if(copr.node == -1) {
          copr match {
            case ReduceOpr(_,true,_)  // total aggregation result is on coordinator
              => copr.node = Executor.coordinator
            case _ => copr.node = opr.node
          }
          if (trace && isCoordinator())
            info("schedule opr "+op+" on node "+copr.node)
          taskQueue.enqueue(op)
        }
      }
    }
  }
}

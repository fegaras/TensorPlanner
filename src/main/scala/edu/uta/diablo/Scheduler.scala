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
import scala.collection.mutable.ListBuffer


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

  // calculate and store the static b_level of each node
  def set_blevel[I,T,S] ( e: Plan[I,T,S] ) {
    def set_blevel ( opr: Opr, blevel: Int ) {
      if (blevel > opr.static_blevel) {
        opr.static_blevel = blevel
        children(opr).foreach{ c => set_blevel(operations(c),blevel+1) }
      }
    }
    val exit_points = e._3.map(x => x._2._3)
    exit_points.foreach{ c => set_blevel(operations(c),0) }
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
    Runtime.DFS(s,
        c => if (operations(c).isInstanceOf[LoadOpr])
               if (!b.contains(c))
                 b.append(c))
    b.toList
  }

  // assign every operation to an executor
  def schedule[I,T,S] ( e: Plan[I,T,S] ) {
    set_sizes()
    set_blevel(e)
    ready_pool = ListBuffer()
    work = 0.until(Communication.num_of_masters).map(w => 0).toArray
    tasks = 0.until(Communication.num_of_masters).map(w => 0).toArray
    val ep = entry_points(e._3.map(x => x._2._3))
    if (isCoordinator())
      info("Entry points: "+ep)
    for ( op <- ep
          if operations(op).node < 0
          if !ready_pool.contains(op) )
       ready_pool += op
    while (ready_pool.nonEmpty) {
      // choose a worker that has done the least work
      val w = work.zipWithIndex.minBy{ case (x,i) => (x,tasks(i)) }._2
      // choose opr with the least communication cost; if many, choose one with the highest b_level
      val c = ready_pool.minBy{ x => val opr = operations(x)
                                     ( communication_cost(opr,w),
                                       -opr.static_blevel ) }
      val opr = operations(c)
      // opr is allocated to worker w
      opr match {
        case ReduceOpr(_,true,_)  // total aggregation
          => opr.node = 0
        case _ => opr.node = w
      }
      work(w) += cpu_cost(opr) + communication_cost(opr,w)
      tasks(w) += 1
      if (isCoordinator())
        info("schedule opr "+c+" on node "+w+" (work = "+work(w)+")")
      ready_pool -= c
      // add more ready nodes
      for { c <- opr.consumers } {
        val copr = operations(c)
        if (copr.node < 0
            && !ready_pool.contains(c)
            && children(copr).forall(operations(_).node >= 0))
          ready_pool += c
      }
    }
    if (trace && isCoordinator())
      print_plan(e)
  }
}

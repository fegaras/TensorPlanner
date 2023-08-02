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
import Communication._

object Runtime {
  val trace = true

  var operations: Array[Opr] = _

  var exit_points: List[Int] = Nil

  def compute ( opr_id: OprID ) {
    if (trace)
      println("*** computing operation "+opr_id+":"+operations(opr_id)+" on node "+myrank)
    val res = operations(opr_id) match {
        case LoadOpr(_,b:(()=>Any))
          => b()
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             assert(operations(x).cached != null)
             f(operations(x).cached).head
        case opr@TupleOpr(x,y)
          => def f ( lg: Opr ): (Any,Any)
               = lg match {
                    case TupleOpr(x,y)
                      => (f(operations(x)),f(operations(y)))
                  case _ => assert(lg.cached != null)
                            lg.cached.asInstanceOf[(Any,Any)]
                 }
             val gx@(iv,_) = f(opr)
             (iv,gx)
        case ReduceOpr(s,fid)
          => val sv = s.map{ x => assert(operations(x).cached != null)
                                  operations(x).cached.asInstanceOf[(Any,Any)] }
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    if (trace)
      println("*-> result of operation "+opr_id+":"+operations(opr_id)+" on node "+myrank+" is "+res)
    operations(opr_id).cached = res
  }

  def enqueue_ready_operations ( opr_id: OprID ) {
    val opr = operations(opr_id)
    var remote: List[Int] = Nil
    for ( c <- opr.consumers ) {
      val copr = operations(c)
      if (opr.node == myrank && copr.node != myrank) {
        if (!remote.contains(copr.node))
          remote = copr.node::remote
      } else if (copr.cached == null
                 && children(copr).map(operations(_)).forall(_.cached != null)
                 && !executors(myrank).ready_queue.contains(c))
               executors(myrank).ready_queue += c
    }
    if (trace && remote.nonEmpty)
      println("    sending the result of "+opr_id+" to nodes "+remote.mkString(", "))
    for ( n <- remote )
      executors(myrank).send_data(n,opr.cached,opr_id)
  }

  class Worker extends Thread {
    override def run () {
      var exit: Boolean = false
      var count: Int = 0
      while (!exit) {
        executors(myrank).check_communication()
        count = (count+1)%100   // check for exit every 100 iterations
        if (count == 0)
          exit = executors(myrank).exit_poll()
        if (executors(myrank).ready_queue.nonEmpty) {
          val opr_id = executors(myrank).ready_queue.head
          if (operations(opr_id).cached == null) {
            compute(opr_id)
            enqueue_ready_operations(opr_id)
          }
          executors(myrank).ready_queue.dequeue
        }
      }
    }
  }

  def entry_points ( s: List[OprID] ): List[OprID]
    = s.flatMap{
        opr => val x = operations(opr)
               if (x.cached == null)
                 if (x.node == myrank && x.isInstanceOf[LoadOpr])
                   List(opr)
                 else entry_points(children(x))
               else List(opr)
      }.distinct

  def schedule[I,T,S] ( e: Plan[I,T,S] ) {
    if (myrank == 0) {
      Scheduler.schedule(e)
      operations = Scheduler.operations.toArray
    }
    executors(myrank).broadcast_plan()
  }

  def eval[I,T,S] ( e: Plan[I,T,S] ): Plan[I,T,S] = {
    exit_points = executors(myrank).broadcast_exit_points(e)
    if (trace && myrank == 0)
      println("Exit points "+exit_points)
    executors(myrank).ready_queue.clear()
    executors(myrank).ready_queue ++= entry_points(exit_points)
    if (trace && myrank == 0)
      println("Entry points "+executors(myrank).ready_queue)
    if (trace)
      println("Queue "+myrank+" "+executors(myrank).ready_queue)
    new Worker().run()
    comm.barrier()
    e
  }

  def collect[I,T,S] ( e: Plan[I,T,S] ): List[Any] = {
    exit_points = executors(myrank).broadcast_exit_points(e)
    val count = exit_points.filter {
        opr_id => myrank == 0 && operations(opr_id).node != 0 }.length
    exit_points.foreach {
        opr_id => val opr = operations(opr_id)
                  if (myrank != 0 && opr.node == myrank)
                    executors(myrank).send_data(0,opr.cached,opr_id)
                  else if (opr.node < 0)
                    throw new Error("Unscheduled node: "+opr_id+" "+opr)
        }
    var c: Int = 0
    if (myrank == 0)
      while (c < count)
          c += executors(myrank).check_communication()
    comm.barrier()
    exit_points.map(x => operations(x).cached)
  }
}

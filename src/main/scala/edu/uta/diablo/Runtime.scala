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
import scala.collection.mutable.Queue

object Runtime {
  val queue = Queue(1000)

  var plan: Array[Opr] = _

  def compute ( opr: Opr ) {
    val res = opr match {
        case LoadOpr(_,b:(()=>Any))
          => b()
        case ApplyOpr(x,fid)
          => val f = function_code(fid).asInstanceOf[Any=>List[Any]]
             f(operations(x).cached).head
        case TupleOpr(x,y)
          => def f ( lg: Opr ): (Any,Any)
               = lg match {
                    case TupleOpr(x,y)
                      => (f(operations(x)),f(operations(y)))
                  case _ => lg.cached.asInstanceOf[(Any,Any)]
                 }
             val gx@(iv,_) = f(opr)
             (iv,gx)
        case ReduceOpr(s,fid)
          => val sv = s.map(operations(_).cached.asInstanceOf[(Any,Any)])
             val op = function_code(fid).asInstanceOf[((Any,Any))=>Any]
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    opr.cached = res
  }

  class Coordinator extends Thread {
    override def run () {
    }
  }

  def enqueue_ready_operations ( opr: Opr ) {
    for ( c <- opr.consumers ) {
      val copr = operations(c)
      if (copr.node != myrank)
        send(copr.node,opr.cached,c,()=>())
      else if (depends(copr).map(operations(_)).forall(_.cached != null))
             queue += c
    }
  }

  class Worker extends Thread {
    override def run () {
      while (queue.nonEmpty) {
        check_communication()
        val opr = operations(queue.dequeue)
        compute(opr)
        enqueue_ready_operations(opr)
      }
    }
  }

  def initialize ( args: Array[String] ) {
    mpi_startup(args)
    if (myrank == 0)
       new Coordinator().run()
    new Worker().run()
  }

  def eval_plan () {
    plan = broadcast_plan(plan)
    for ( i <- 0 until plan.length-1 )
       if (plan(i).node == myrank && plan(i).isInstanceOf[LoadOpr])
         queue += i
  }
}

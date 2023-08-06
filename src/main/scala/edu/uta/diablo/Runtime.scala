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
        case LoadOpr(_,b)
          => b
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             assert(operations(x).cached != null)
             f(operations(x).cached).head
        case opr@TupleOpr(x,y)
          => def f ( lg: Opr ): (Any,Any)
               = lg match {
                    case TupleOpr(x,y)
                      => val gx@(iv,vx) = f(operations(x))
                         val gy@(_,vy) = f(operations(y))
                         val xx = if (operations(x).isInstanceOf[TupleOpr]) vx else gx
                         val yy = if (operations(y).isInstanceOf[TupleOpr]) vy else gy
                         (iv,(xx,yy))
                    case _ => assert(lg.cached != null)
                              lg.cached.asInstanceOf[(Any,Any)]
                 }
             f(opr)
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

  // distributed evaluation of a scheduled plan using MPI
  def eval[I,T,S] ( plan: Plan[I,T,S] ): Plan[I,T,S] = {
    exit_points = executors(myrank).broadcast_exit_points(plan)
    if (trace && myrank == 0)
      println("Exit points "+exit_points)
    executors(myrank).ready_queue.clear()
    executors(myrank).ready_queue ++= entry_points(exit_points)
    if (trace && myrank == 0)
      println("Entry points "+executors(myrank).ready_queue)
    if (trace)
      println("Queue "+myrank+" "+executors(myrank).ready_queue)
    new Worker().run()
    barrier()
    plan
  }

  // collect the results of an evaluated plan at the master node
  def collect[I,T,S] ( plan: Plan[I,T,S] ): List[(I,Any)] = {
    exit_points = executors(myrank).broadcast_exit_points(plan)
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
    barrier()
    exit_points.map(x => operations(x).cached.asInstanceOf[(I,Any)])
  }

/****************************************************************************************************
* 
* Single core, in-memory evalution (for testing only)
* 
****************************************************************************************************/

  var cached_blocks: Int = 0
  var max_cached_blocks: Int = 0

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

  def eval_mem ( id: OprID, tabs: Int ): Any = {
    val e = operations(id)
    if (e.cached != null) {
      // retrieve result block(s) from cache
      val res = e.cached
      e.count -= 1
      if (e.count <= 0) {
        if (trace)
          println(" "*tabs*3+"* "+"discard the cached value of "+id)
        cached_blocks -= 1
        e.cached = null
      }
      return res
    }
    if (trace)
      println(" "*3*tabs+"*** "+tabs+": "+id+"/"+e)
    val res = e match {
        case LoadOpr(_,b)
          => b
        case ApplyOpr(x,fid)
          => val f = functions(fid).asInstanceOf[Any=>List[Any]]
             f(eval_mem(x,tabs+1)).head
        case TupleOpr(x,y)
          => def f ( lg: OprID ): (Any,Any)
               = operations(lg) match {
                    case TupleOpr(x,y)
                      => val gx@(iv,vx) = f(x)
                         val gy@(_,vy) = f(y)
                         val xx = if (operations(x).isInstanceOf[TupleOpr]) vx else gx
                         val yy = if (operations(y).isInstanceOf[TupleOpr]) vy else gy
                         (iv,(xx,yy))
                    case _ => eval_mem(lg,tabs+1).asInstanceOf[(Any,Any)]
                 }
             f(id)
        case ReduceOpr(s,fid)
          => val sv = s.map(eval_mem(_,tabs+1).asInstanceOf[(Any,Any)])
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             (sv.head._1,sv.map(_._2).reduce{ (x:Any,y:Any) => op((x,y)) })
      }
    if (trace)
      println(" "*3*tabs+"*-> "+tabs+": "+res)   // pe(res)
    e.count -= 1
    if (e.count > 0) {
      e.cached = res
      if (trace)
        println(" "*tabs*3+"* "+"cache the value of "+id)
      cached_blocks += 1
      max_cached_blocks = Math.max(max_cached_blocks,cached_blocks)
    }
    res
  }

  def evalMem[I,T,S] ( e: Plan[I,T,S] ): (T,S,List[(I,Any)])
    = e match {
        case (dp,sp,s)
          => operations = Scheduler.operations.toArray
             cached_blocks = 0
             max_cached_blocks = 0
             operations.foreach(x => x.count = x.consumers.length)
             val res = s.map{ case (i,(ds,ss,lg))
                                => eval_mem(lg,0).asInstanceOf[(I,Any)] }
             println("Number of nodes: "+operations.length)
             println("Max num of cached blocks: "+max_cached_blocks)
             println("Final num of cached blocks: "+cached_blocks)
             (dp,sp,res)
      }
}

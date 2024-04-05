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

import PlanGenerator.{operations=>_,loadBlocks=>_,_}
import Runtime._
import mpi._
import mpi.MPI._
import java.nio.ByteBuffer
import java.io._


@transient
// MPI communicator using openMPI (one executor per node)
object Executor {
  var executor_rank: WorkerID = 0
  var coordinator: WorkerID = 0
  // the buffer must fit few blocks (one block of doubles is 8MBs)
  val max_buffer_size = 500000000
  val buffer: ByteBuffer = newByteBuffer(max_buffer_size)
  var receive_request: Request = _
  var comm: Intracomm = _//MPI.COMM_WORLD
  var enable_recovery: Boolean = false
  var failed_executors: List[WorkerID] = Nil
  var active_ex  }

  def serialization_error ( ex: IOException ) {
    info("Serialization error: "+ex.getMessage)
    ex.printStackTrace()
    comm.abort(-1)
  }

  def error ( msg: String ) {
    info("*** "+msg)
    comm.abort(-1)
  }

  // process the incoming message
  def handle_received_message ( tag: Byte, opr_id: OprID, data: Any, len: Int ) {
    val opr = operations(opr_id)
    if (tag == 0) {  // cache data and check for ready operations
      info("    received "+(len+4)+" bytes at "+executor_rank+" (opr "+opr_id+")")
      cache_data(opr,data)
      opr.status = completed
      enqueue_ready_operations(opr_id)
    } else if (tag == 2) {   // cache data
      info("    received "+(len+4)+" bytes at "+executor_rank)
      cache_data(opr,data)
      opr.status = completed
    } else if (tag == 3) {  // coordinator will decide who will replace a failed executor
      val failed_executor = data.asInstanceOf[Int]
      if (!failed_executors.contains(failed_executor)) {
        skip_work = true
        failed_executors = failed_executor::failed_executors
        active_executors = active_executors.filter(_ != failed_executor)
        val candidates = active_executors.filter(_ != coordinator)
        val new_executor = candidates((Math.random()*candidates.length).toInt)
        info("The coordinator has decided to replace the failed executor "
             +failed_executor+" with "+new_executor)
        send_data(active_executors,(failed_executor,new_executor),0,4)
      }
    } else if (tag == 4) {  // recovery at all executors (sent from coordinator)
      val (failed_executor,new_executor) = data.asInstanceOf[(Int,Int)]
      if (!failed_executors.contains(failed_executor))
        failed_executors = failed_executor::failed_executors
      active_executors = active_executors.filter(_ != failed_executor)
      Runtime.recover(failed_executor,new_executor)
    } else if (tag == 5) {  // synchronize accumulation (on coordinator)
      accumulator.values = data::accumulator.values
      accumulator.count += 1
      if (!skip_work && accumulator.count == active_executors.length) {
        accumulator.total = accumulator.values.reduce(accumulator.acc)
        accumulator.count = 0
        send_data(active_executors,accumulator.total,6)
        //info("Aggregate "+accumulator.values+" = "+accumulator.total)
      }
    } else if (tag == 6) {   // release the barrier synchronization after accumulation
      accumulator.reset()
      accumulator.total = data
    } else if (tag == 1) {   // partial/final reduction
      opr match {
        case ReduceOpr(s,valuep,fid)
          => // partial reduce ReduceOpr
             info("    received partial reduce result of "+(len+4)+" bytes at "
                  +executor_rank+" (opr "+opr_id+")")
             val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
             if (opr.cached == null) {
               stats.cached_blocks += 1
               stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
               opr.cached = data
             } else if (valuep)
                      // total aggregation
                      opr.cached = op((opr.cached,data))
               else { val x = opr.cached.asInstanceOf[(Any,Any)]
                      val y = data.asInstanceOf[(Any,Any)]
                      // merge the current state with the incoming partially reduced data
                      opr.cached = (x._1,op((x._2,y._2)))
                    }
             opr.reduced_count -= 1
             if (opr.reduced_count == 0) {
               // completed ReduceOpr
               info("    completed reduce of opr "+opr_id+" at "+executor_rank)
               stats.reduce_operations += 1
               opr.status = computed
               enqueue_ready_operations(opr_id)
             }
        case _ => ;
      }
    } else error("Unknown received tag: "+tag+" at exec "+executor_rank)
  }

  // check if there is a request to send data (array blocks); if there is, get the data and cache it
  def check_communication (): Int = {
    if (receive_request == null)
      try {
        buffer.clear()
        // prepare for the first receive (non-blocking)
        receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(-1,ex) }
    if (receive_request.test()) {
      // deserialize and cache the incoming data
      val len = buffer.getInt()
      val bb = Array.ofDim[Byte](len)
      buffer.get(bb)
      val bs = new ByteArrayInputStream(bb,0,len)
      val is = new ObjectInputStream(bs)
      val opr_id = is.readInt()
      val tag = is.readByte()
      val data = try { is.readObject()
                     } catch { case ex: IOException
                                 => serialization_error(ex); null }
      is.close()
      try {
        receive_request.free()
        buffer.clear()
        // prepare for the next receive (non-blocking)
        receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(-1,ex) }
      new Thread() {
        override def run () {
          handle_received_message(tag,opr_id,data,len)
        }
      }.start()
      1
    } else 0
  }

  // send the result of an operation (array blocks) to other executors
  def send_data ( ranks: List[WorkerID], data: Any, opr_id: OprID, tag: Int ) {
    if (data == null)
      error("null data sent for "+opr_id+" to "+ranks.mkString(",")+" from "+executor_rank)
    // serialize data into a byte array
    val bs = new ByteArrayOutputStream(max_buffer_size)
    val os = new ObjectOutputStream(bs)
    try {
      os.writeInt(opr_id)
      os.writeByte(tag)
      os.writeObject(data)
      os.close()
    } catch { case ex: IOException
                => serialization_error(ex) }
    val ba = bs.toByteArray
    val bb = newByteBuffer(ba.length+4)
    bb.putInt(ba.length).put(ba)
    info("    sending "+(ba.length+4)+" bytes from "+executor_rank
         +" to "+ranks.mkString(",")+(if (opr_id <= 0) " (tag "+tag+")" else " (opr "+opr_id+")"))
    for ( rank <- ranks )
      try {
        if (!enable_recovery)
          comm.send(bb,ba.length+4,BYTE,rank,tag)
        else {
          var sr = comm.iSend(bb,ba.length+4,BYTE,rank,tag)
          var count = 0
          while (!sr.test() && count < max_wait_time) {
            count += 1
            Thread.sleep(1)
          }
          if (!sr.test() && !Runtime.skip_work) {
            sr.cancel()
            sr.free()
            throw new MPIException("Executor "+rank+" is not responding")
          } else sr.free()
        }
      } catch { case ex: MPIException
                  => mpi_error(rank,ex) }
  }

  // send data to executors
  def send_data ( ranks: List[WorkerID], data: Any, tag: Int ) {
    // serialize data into a byte array
    val bs = new ByteArrayOutputStream(max_buffer_size)
    val os = new ObjectOutputStream(bs)
    try {
      os.writeInt(0)
      os.writeByte(tag)
      os.writeObject(data)
      os.close()
    } catch { case ex: IOException
                => serialization_error(ex) }
    val ba = bs.toByteArray
    val bb = newByteBuffer(ba.length+4)
    bb.putInt(ba.length).put(ba)
    for ( rank <- ranks )
      try {
        comm.send(bb,ba.length+4,BYTE,rank,tag)
      } catch { case ex: MPIException
                  => mpi_error(rank,ex) }
  }

  // receive the result of an operation (array blocks) from another executor
  def receive_data ( rank: WorkerID ): Any = {
    buffer.clear()
    try {
      comm.recv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
    } catch { case ex: MPIException
                => mpi_error(rank,ex) }
    val len = buffer.getInt()
    val bb = Array.ofDim[Byte](len)
    buffer.get(bb)
    val bs = new ByteArrayInputStream(bb,0,len)
    val is = new ObjectInputStream(bs)
    val opr_id = is.readInt()
    val tag = is.readByte()
    val data = try { is.readObject()
                   } catch { case ex: IOException
                               => serialization_error(ex); null }
    info("    received "+(len+4)+" bytes at "+executor_rank
         +" from "+rank+" (opr "+opr_id+")")
    is.close()
    operations(opr_id).cached = data
    operations(opr_id).status = completed
    data
  }
 
  def broadcast_plan () {
    try {
      if (isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeInt(loadBlocks.length)
        os.writeObject(operations)
        os.writeObject(functions)
        os.close()
        val a = bs.toByteArray
        comm.bcast(Array(a.length),1,INT,0)
        comm.bcast(a,a.length,BYTE,0)
        for ( opr_id <- operations.indices ) {
          val x = operations(opr_id)
          x match {
            case LoadOpr(b)
              if x.node > 0
              => send_data(List(x.node),loadBlocks(b),opr_id,0)
            case _ => ;
          }
        }
      } else {
        val len = Array(0)
        comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.length)
        val is = new ObjectInputStream(bs)
        val lb_len = is.readInt()
        loadBlocks = Array.ofDim[Any](lb_len)
        operations = is.readObject().asInstanceOf[Array[Opr]]
        functions = is.readObject().asInstanceOf[Array[Nothing=>Any]]
        is.close()
        for ( x <- operations ) {
          x match {
            case LoadOpr(b)
              if x.node == executor_rank
              => loadBlocks(b) = receive_data(0)
            case _ => ;
          }
        }
      }
      comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(-1,ex)
              case ex: IOException
                => serialization_error(ex) }
  }

  def broadcast ( value: Any ): Any = {
    var result = value
    try {
      if (isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(value)
        os.flush()
        os.close()
        val a = bs.toByteArray
        comm.bcast(Array(a.length),1,INT,0)
        comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        comm.bcast(len,1,INT,0)
        val buffer = Array.fill[Byte](len(0))(0)
        comm.bcast(buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(buffer,0,buffer.length)
        val is = new ObjectInputStream(bs)
        result = is.readObject()
        is.close()
      }
      comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(-1,ex)
              case ex: IOException
                => serialization_error(ex) }
    result
  }

  def broadcast_exit_points ( es: List[OprID] ): List[OprID] = {
    exit_points = broadcast(if (isCoordinator()) es else null).asInstanceOf[List[OprID]]
    exit_points
  }

  // accumulate values at the coordinator O(n)
  def accumulate[T] ( value: T, acc: (T,T) => T ): T = {
    send_data(List(coordinator),value,5)
    accumulator.acc = acc.asInstanceOf[(Any,Any)=>Any]
    accumulator.exit = false
    while ( !accumulator.exit )
      Thread.sleep(10)
    accumulator.total.asInstanceOf[T]
  }

  // exit when the queues of all executors are empty and exit points have been computed
  def exit_poll (): Boolean = {
    def and ( x: Boolean, y: Boolean ) = x && y
    if (skip_work)
      false
    else {
      val b = (ready_queue.isEmpty
               && send_queue.isEmpty
               && exit_points.forall {
                     x => val opr = operations(x)
                          opr.node != executor_rank || hasCachedValue(opr) })
      if (enable_recovery) {
        if (!b)
          info("    cannot exit at "+executor_rank+": needs "+exit_points.filter {
                     x => val opr = operations(x)
                          opr.node == executor_rank && !hasCachedValue(opr) })
        accumulate(b,and)
      } else try {
        val in = Array[Byte](if (b) 0 else 1)
        val ret = Array[Byte](0)
        comm.allReduce(in,ret,1,BYTE,LOR)
        ret(0) == 0
      } catch { case ex: MPIException
                  => mpi_error(-1,ex); false }
    }
  }

  def collect_statistics (): Statistics = {
    def plus ( x: Int, y: Int ) = x+y
    def max ( x: Int, y: Int ) = Math.max(x,y)
    if (enable_recovery)
      new Statistics(accumulate(stats.apply_operations,plus),
                     accumulate(stats.reduce_operations,plus),
                     accumulate(stats.cached_blocks,plus),
                     accumulate(stats.max_cached_blocks,max))
    else try {
      val in = Array[Int](stats.apply_operations,
                          stats.reduce_operations,
                          stats.cached_blocks)
      val res = Array.fill[Int](in.length)(0)
      comm.reduce(in,res,in.length,INT,SUM,0)
      val max_in = Array(stats.max_cached_blocks)
      val max = Array(0)
      comm.reduce(max_in,max,1,INT,MAX,0)
      new Statistics(res(0),res(1),res(2),max(0))
    } catch { case ex: MPIException
                => mpi_error(-1,ex); stats }
  }
}


object Communication {
  import Executor._
  var num_of_executors: Int = 1

  def barrier () {
    def or ( x: Boolean, y: Boolean ) = x || y
    if (!enable_recovery)
      comm.barrier()    // O(logn)
    else accumulate(false,or)  // O(n)
  }

  def isCoordinator (): Boolean
    = (executor_rank == coordinator)

  def mpi_startup ( args: Array[String] ) {
    try {
      InitThread(args,THREAD_FUNNELED)
      comm = MPI.COMM_WORLD
      // don't abort on MPI errors
      comm.setErrhandler(ERRORS_RETURN)
      executor_rank = comm.getRank
      num_of_executors = comm.getSize
      active_executors = 0.until(num_of_executors).toList
      val local_rank = System.getenv("OMPI_COMM_WORLD_LOCAL_RANK").toInt
      info("Using executor "+executor_rank+": "+getProcessorName+"/"+local_rank+" with "
           +(java.lang.Runtime.getRuntime.availableProcessors())+" Java threads and "
           +(java.lang.Runtime.getRuntime.totalMemory()/1024/1024/1024)+" GBs")
    } catch { case ex: MPIException
                => info("MPI error: "+ex.getMessage)
                   ex.printStackTrace()
                   comm.abort(-1)
            }
  }

  def mpi_finalize () {
    info("Executor "+executor_rank+" is exiting")
    if (!enable_recovery)
      Finalize()
    else System.exit(0)
  }

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

import Scheduler.{operations=>_,_}
import mpi._
import mpi.MPI._
import java.nio.ByteBuffer
import scala.collection.mutable.SynchronizedQueue
import java.io._


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
    println("Number of apply operations: "+apply_operations)
    println("Number of reduce operations: "+reduce_operations)
    println("Max num of cached blocks: "+max_cached_blocks)
    println("Final num of cached blocks: "+cached_blocks)
  }
}


@transient
// MPI communicator using openMPI (one executor per node)
object Executor {
  import Communication._
  // the buffer must fit few blocks (one block of doubles is 8MBs)
  val max_buffer_size = 100000000
  var buffer = newByteBuffer(max_buffer_size)
  // operations ready to be executed
  var ready_queue: SynchronizedQueue[Int] = new SynchronizedQueue[Int]()
  var receive_request: Request = null
  val stats: Statistics = new Statistics()
  var exit_points: List[Int] = Nil
  val comm = MPI.COMM_WORLD
  var master_comm: mpi.Comm = comm

  def mpi_error ( ex: MPIException ) {
    System.err.println("MPI error: "+ex.getMessage())
    System.err.println("  Error class: "+ex.getErrorClass())
    ex.printStackTrace()
    comm.abort(-1)
  }

  def serialization_error ( ex: IOException ) {
    System.err.println("Serialization error: "+ex.getMessage())
    ex.printStackTrace()
    comm.abort(-1)
  }

  def comm_error ( status: Status ) {
    val node = status.getSource
    System.err.println("*** "+status.getError())
    // TODO: needs recovery instead of abort
    comm.abort(-1)
  }

  // check if there is a request to send data (array blocks); if there is, get the data and cache it
  def check_communication (): Int = {
    if (receive_request == null)
      try {
        buffer.clear()
        // prepare for the first receive (non-blocking)
        receive_request = master_comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(ex) }
    if (receive_request.test()) {
      // deserialize and cache the incoming data
      val len = buffer.getInt()
      // Not working: val len = receive_request.getStatus().getCount(BYTE)
      val bb = Array.ofDim[Byte](len)
      buffer.get(bb)
      val bs = new ByteArrayInputStream(bb,0,len)
      val is = new ObjectInputStream(bs)
      val opr_id = is.readInt()
      val data = try { is.readObject()
                     } catch { case ex: IOException
                                 => serialization_error(ex); null }
      if (trace)
        println("    received "+(len+4)+" bytes from "+Runtime.operations(opr_id).node
                +" to "+my_master_rank+" (opr "+opr_id+")")
      is.close()
      Runtime.operations(opr_id).cached = data
      try {
        receive_request.free()
        buffer.clear()
        // prepare for the next receive (non-blocking)
        receive_request = master_comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(ex) }
      Runtime.enqueue_ready_operations(opr_id)
      Runtime.check_caches(opr_id)
      1
    } else 0
  }

  // send the result of an operation (array blocks) to another node
  def send_data ( rank: Int, data: Any, oper_id: OprID ) {
      // serialize data into a byte array
      val bs = new ByteArrayOutputStream(max_buffer_size)
      val os = new ObjectOutputStream(bs)
      try {
        os.writeInt(oper_id)
        os.writeObject(data)
        os.flush()
	os.close()
      } catch { case ex: IOException
                  => serialization_error(ex) }
      val ba = bs.toByteArray()
      val bb = newByteBuffer(ba.length+4)
      bb.putInt(ba.length).put(ba)
      if (trace)
	println("    sending "+(ba.length+4)+" bytes from "+my_master_rank+" to "
                +rank+" (opr "+oper_id+")")
      try {
        master_comm.send(bb,ba.length+4,BYTE,rank,my_master_rank)
      } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  def broadcast_plan () {
    try {
      if (Communication.isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(Runtime.operations)
        os.writeObject(functions)
        os.flush()
        os.close()
        val a = bs.toByteArray()
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        master_comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        master_comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.size)
        val is = new ObjectInputStream(bs)
        Runtime.operations = is.readObject().asInstanceOf[Array[Opr]]
        functions = is.readObject().asInstanceOf[Array[Nothing=>Any]]
        is.close()
      }
      master_comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(ex)
              case ex: IOException
                => serialization_error(ex) }
  }

  def broadcast_exit_points[I,T,S] ( e: Plan[I,T,S] ): List[Int] = {
    try {
      if (Communication.isCoordinator()) {
        exit_points = e._3.map(x => x._2._3)
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(exit_points)
        os.flush()
        os.close()
        val a = bs.toByteArray()
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        master_comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        master_comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.size)
        val is = new ObjectInputStream(bs)
        exit_points = is.readObject().asInstanceOf[List[Int]]
        is.close()
      }
      master_comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(ex)
              case ex: IOException
                => serialization_error(ex) }
    exit_points
  }

  // exit when the queues of all executors are empty and exit points have been computed
  def exit_poll (): Boolean = {
    try {
      val b = (ready_queue.isEmpty
               && exit_points.forall{ x => val opr = Runtime.operations(x)
                                           opr.node != my_master_rank || opr.cached != null })
      val in = Array[Byte](if (b) 0 else 1)
      val ret = Array[Byte](0)
      master_comm.allReduce(in,ret,1,BYTE,LOR)
      ret(0) == 0
    } catch { case ex: MPIException
                => mpi_error(ex); false }
  }

  def collect_statistics (): Statistics
    = try {
        val in = Array[Int](stats.apply_operations,
                            stats.reduce_operations,
                            stats.cached_blocks,
                            stats.max_cached_blocks)
        val res = Array.fill[Int](in.size)(0)
        master_comm.reduce(in,res,in.size,INT,SUM,0)
        new Statistics(res(0),res(1),res(2),res(3))
      } catch { case ex: MPIException
                  => mpi_error(ex); stats }
}


object Communication {
  import Executor._
  var my_world_rank: Int = 0
  var i_am_master: Boolean = false
  var my_master_rank: Int = -1
  var num_of_masters: Int = 0

  def barrier () { master_comm.barrier() }

  // One master per node (can do MPI)
  def isMaster (): Boolean = i_am_master

  // Single coordinator
  def isCoordinator (): Boolean = (my_master_rank == 0)

  def mpi_startup ( args: Array[String] ) {
    try {
      // Need one MPI thread per worker node (the Master)
      InitThread(args,THREAD_FUNNELED)
      // don't abort on MPI errors
      //comm.setErrhandler(ERRORS_RETURN)
      my_world_rank = comm.getRank()
      i_am_master = (System.getenv("OMPI_COMM_WORLD_LOCAL_RANK") == "0")
      // allow only masters to communicate via MPI
      master_comm = comm.split(if (isMaster()) 1 else UNDEFINED,my_world_rank)
      if (isMaster()) {
        my_master_rank = master_comm.getRank()
        num_of_masters = master_comm.getSize()
        val available_threads = System.getenv("OMPI_COMM_WORLD_LOCAL_SIZE")
        println("Using master "+my_master_rank+": "+getProcessorName()+"/"+my_world_rank
                +" with "+available_threads+" threads")
      }
    } catch { case ex: MPIException
                => System.err.println("MPI error: "+ex.getMessage())
                   ex.printStackTrace()
                   comm.abort(-1)
            }
  }

  def mpi_finalize () {
    Finalize()
  }
}

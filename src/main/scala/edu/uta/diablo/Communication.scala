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
import scala.collection.mutable.Queue
import java.io._

class Executor ( myrank: Int ) {
  // the buffer must fit few blocks (one block of doubles is 8MBs)
  val max_buffer_size = 100000000
  var buffer = newByteBuffer(max_buffer_size)
  // operations ready to be executed
  var ready_queue: Queue[Int] = new Queue[Int]()
  var receive_request: Request = null
  var exit_points: List[Int] = Nil
  val comm = MPI.COMM_WORLD

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

  def check_communication (): Int = {
    if (receive_request == null)
      try {
        buffer.clear()
        // prepare for the first receive (non-blocking)
        receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
        0
      } catch { case ex: MPIException
                  => mpi_error(ex); 0 }
    else if (receive_request.test()) {
        // deserialize and cache the incoming data
        val len = buffer.getInt()
        val bb = Array.ofDim[Byte](len)
        buffer.get(bb)
        val bs = new ByteArrayInputStream(bb,0,len)
        val is = new ObjectInputStream(bs)
        val opr_id = is.readInt()
        val data = try { is.readObject()
                       } catch { case ex: IOException
                                   => serialization_error(ex); null }
        is.close()
        Runtime.operations(opr_id).cached = data
        try {
          receive_request.free()
          buffer.clear()
          // prepare for the next receive (non-blocking)
          receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
        } catch { case ex: MPIException
                    => mpi_error(ex) }
        Runtime.enqueue_ready_operations(opr_id)
        1
    } else 0
  }

  def send_data ( rank: Int, data: Any, oper_id: OprID ) {
    // serialize data into a byte array
    val bs = new ByteArrayOutputStream(1000000)
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
    try {
      comm.send(bb,ba.length+4,BYTE,rank,myrank)
    } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  def receive_data ( rank: Int ) {
    buffer.clear()
    try {
      comm.recv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
    } catch { case ex: MPIException
                => mpi_error(ex) }
    val len = buffer.getInt()
    val bb = Array.ofDim[Byte](len)
    buffer.get(bb)
    val bs = new ByteArrayInputStream(bb,0,len)
    val is = new ObjectInputStream(bs)
    val opr_id = is.readInt()
    val data = try { is.readObject()
                   } catch { case ex: IOException
                               => serialization_error(ex); null }
    is.close()
    Runtime.operations(opr_id).cached = data
  }

  def broadcast_plan () {
    try {
      if (myrank == 0) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(Runtime.operations)
        os.writeObject(functions)
        os.flush()
        os.close()
        val a = bs.toByteArray()
        comm.bcast(Array(a.length),1,INT,0)
        comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.size)
        val is = new ObjectInputStream(bs)
        Runtime.operations = is.readObject().asInstanceOf[Array[Opr]]
        functions = is.readObject().asInstanceOf[Array[Nothing=>Any]]
        is.close()
      }
      comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(ex)
              case ex: IOException
                => serialization_error(ex) }
  }

  def broadcast_exit_points[I,T,S] ( e: Plan[I,T,S] ): List[Int] = {
    try {
      if (myrank == 0) {
        exit_points = e._3.map(x => x._2._3)
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(exit_points)
        os.flush()
        os.close()
        val a = bs.toByteArray()
        comm.bcast(Array(a.length),1,INT,0)
        comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.size)
        val is = new ObjectInputStream(bs)
        exit_points = is.readObject().asInstanceOf[List[Int]]
        is.close()
      }
      comm.barrier()
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
                                           opr.node != myrank || opr.cached != null })
      val in = Array[Byte](if (b) 0 else 1)
      val ret = Array[Byte](0)
      comm.allReduce(in,ret,1,BYTE,LOR)
      ret(0) == 0
    } catch { case ex: MPIException
                => mpi_error(ex); false }
  }
}

object Communication {
  var myrank: Int = 0
  var num_of_workers: Int = 0
  var executors: Array[Executor] = _
  val comm = MPI.COMM_WORLD

  def mpi_startup ( args: Array[String] ) {
    try {
      Init(args)
      // don't abort on MPI errors
      comm.setErrhandler(ERRORS_RETURN)
      myrank = comm.getRank()
      num_of_workers = comm.getSize()
      if (executors == null)
        executors = Array.ofDim[Executor](num_of_workers)
      executors(myrank) = new Executor(myrank)
      println("Using executor "+getProcessorName()+"/"+myrank)
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

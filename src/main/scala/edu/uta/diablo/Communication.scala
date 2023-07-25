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
import mpi._
import mpi.MPI._
import java.nio.ByteBuffer
import java.io._
import scala.collection.mutable.DoubleLinkedList

object Communication {
  // the buffer must fit few blocks (a block of doubles is 8MBs)
  val max_buffer_size = 100000000
  var myrank: Int = 0
  var num_of_workers: Int = 0
  val comm = MPI.COMM_WORLD
  val buffer = newByteBuffer(max_buffer_size)

  var receive_request: Request = null

  // each open send request has a callback function
  var open_requests = DoubleLinkedList[(Request,()=>Any)]()

  def mpi_error ( ex: MPIException ) {
    println("MPI error: "+ ex.getMessage())
    println("  Error class: "+ ex.getErrorClass())
    ex.printStackTrace()
    comm.abort(-1)
  }

  def serialization_error ( ex: IOException ) {
    println("Serialization error: "+ ex.getMessage())
    ex.printStackTrace()
    comm.abort(-1)
  }

  def comm_error ( status: Status ) {
    val node = status.getSource
    println("*** "+status.getError())
    // TODO: needs recovery instead of abort
    comm.abort(-1)
  }

  def check_communication () {
    if (receive_request == null)
      try {
        // prepare for the first receive (non-blocking)
        receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(ex) }
    else if (receive_request.test) {
        // deserialize and cache the incoming data
        val status = receive_request.testStatus()
        val bs = new ByteArrayInputStream(buffer.array(),0,status.getCount(BYTE))
        val is = new ObjectInputStream(bs)
        val data = try { is.readObject()
                       } catch { case ex: IOException
                                   => serialization_error(ex); null }
        is.close()
        // the receive tag is the operation id
        val opr_id = status.getTag
        operations(opr_id).cached = data
        try {
          receive_request.free()
          // prepare for the next receive (non-blocking)
          receive_request = comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
        } catch { case ex: MPIException
                    => mpi_error(ex) }
        Runtime.enqueue_ready_operations(operations(opr_id))
    }
    val it = open_requests.iterator
    // check for completed non-blocking send requests
    while ( it.hasNext ) {
      val (req,callback) = it.next
      if (req.test) {
        val status = req.testStatus()
        if (status.getError() == SUCCESS) {
          // call the callback function of the completed write
          callback()
          req.free()
          open_requests.remove()
        } else comm_error(status)
      }
    }
  }

  def send ( rank: Int, data: Any, tag: Int, callback: ()=>Any ) {
    // serialize data into a byte array
    val bs = new ByteArrayOutputStream(10000)
    val os = new ObjectOutputStream(bs)
    try {
      os.writeObject(data)
      os.flush()
      os.close()
    } catch { case ex: IOException
                => serialization_error(ex) }
    val ba = bs.toByteArray()
    try {
      // send data asynchronously
      val rq = comm.iSend(ByteBuffer.wrap(ba),ba.length+4,BYTE,rank,tag)
      // store send requests (to callback when done and to check if receiver is dead)
      open_requests = open_requests:+((rq,callback))
    } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  def broadcast_plan ( plan: Array[Opr] ): Array[Opr]
    = try {
        if (myrank == 0) {
          val bs = new ByteArrayOutputStream(10000)
          val os = new ObjectOutputStream(bs)
          os.writeObject(plan)
          os.flush()
          os.close()
          val a = bs.toByteArray()
          comm.bcast(Array(a.length),1,INT,0)
          comm.bcast(a,a.length,BYTE,0)
          plan
        } else {
          val len = Array(0)
          comm.bcast(len,1,INT,0)
          val plan_buffer = Array.fill[Byte](len(0))(0)
          comm.bcast(plan_buffer,len(0),BYTE,0)
          val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.size)
          val is = new ObjectInputStream(bs)
          val data = is.readObject()
          is.close()
          data.asInstanceOf[Array[Opr]]
        }
      } catch { case ex: MPIException
                  => mpi_error(ex); null
                case ex: IOException
                  => serialization_error(ex); null }

  def mpi_startup ( args: Array[String] ) {
    try {
      // Need one MPI thread per worker node
      InitThread(args,THREAD_FUNNELED)
      // don't abort on MPI errors
      comm.setErrhandler(ERRORS_RETURN)
      myrank = comm.getRank()
      num_of_workers = comm.getSize()
    } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  def main ( args: Array[String] ) {
    Init(args)
    myrank = comm.getRank()
    num_of_workers = comm.getSize()
    var plan: Array[Opr] = Array(LoadOpr(10,()=>()))
    val p = broadcast_plan(plan)
    println("### "+myrank+" "+p(0))
    Finalize()
  }
}

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

import PlanGenerator.{operations=>_,_}
import mpi._
import mpi.MPI._
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue
import java.io._


@transient
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
    info("Number of apply operations: "+apply_operations)
    info("Number of reduce operations: "+reduce_operations)
    info("Max num of cached blocks per node: "+max_cached_blocks)
    info("Final num of cached blocks: "+cached_blocks)
  }
}

@transient
object operator_comparator extends Comparator[Int] {
  def priority ( opr_id: Int ): Int
    = Runtime.operations(opr_id) match {
        case TupleOpr(_,_) => 4
        case ReduceOpr(_,_) => 3
        case LoadOpr(_,_) => 2
        case _ => 1
      }
  override def compare ( x: Int, y: Int ): Int
    = priority(x) - priority(y)
}

@transient
// MPI communicator using openMPI (one executor per node)
object Executor {
  import Communication.my_master_rank
  // the buffer must fit few blocks (one block of doubles is 8MBs)
  val max_buffer_size = 100000000
  val buffer: ByteBuffer = newByteBuffer(max_buffer_size)
  // operations ready to be executed
  var ready_queue = new PriorityBlockingQueue(1000,operator_comparator)
  var receive_request: Request = _
  val stats: Statistics = new Statistics()
  var exit_points: List[Int] = Nil
  val comm: Intracomm = MPI.COMM_WORLD
  var master_comm: mpi.Comm = comm

  def mpi_error ( ex: MPIException ) {
    System.err.println("MPI error: "+ex.getMessage)
    System.err.println("  Error class: "+ex.getErrorClass)
    ex.printStackTrace()
    comm.abort(-1)
  }

  def serialization_error ( ex: IOException ) {
    System.err.println("Serialization error: "+ex.getMessage)
    ex.printStackTrace()
    comm.abort(-1)
  }

  def comm_error ( status: Status ) {
    val node = status.getSource
    System.err.println("*** "+status.getError)
    // TODO: needs recovery instead of abort
    comm.abort(-1)
  }

  def error ( msg: String ) {
    System.err.println("*** "+msg)
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
      val tag = is.readByte()
      val data = try { is.readObject()
                     } catch { case ex: IOException
                                 => serialization_error(ex); null }
      is.close()
      try {
        receive_request.free()
        buffer.clear()
        // prepare for the next receive (non-blocking)
        receive_request = master_comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(ex) }
      val opr = Runtime.operations(opr_id)
      if (tag == 0) {
        info("    received "+(len+4)+" bytes at "+my_master_rank+" (opr "+opr_id+")")
        Runtime.cache_data(opr,data)
        Runtime.enqueue_ready_operations(opr_id)
      } else if (tag == 2) {
        info("    received "+(len+4)+" bytes at "+my_master_rank)
        Runtime.cache_data(opr,data)
      } else if (tag == 1) {
        opr match {
          case ReduceOpr(s,fid)  // partial reduce
            => // partial reduce copr ReduceOpr
               info("    received partial reduce result of "+(len+4)+" bytes at "
                    +my_master_rank+" (opr "+opr_id+")")
               val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
               if (opr.cached == null) {
                 stats.cached_blocks += 1
                 stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
                 opr.cached = data
               } else { val x = opr.cached.asInstanceOf[(Any,Any)]
                        val y = data.asInstanceOf[(Any,Any)]
                        opr.cached = (x._1,op((x._2,y._2)))
                      }
               opr.reduced_count -= 1
               if (opr.reduced_count == 0) {
                 // completed ReduceOpr
                 info("    completed reduce of opr "+opr_id+" at "+my_master_rank)
                 stats.reduce_operations += 1
                 opr.completed = true
                 Runtime.enqueue_ready_operations(opr_id)
               }
          case _ => ;
        }
      }
      1
    } else 0
  }

  // send the result of an operation (array blocks) to another node
  def send_data ( rank: Int, data: Any, oper_id: OprID, tag: Int = 0 ) {
    if (data == null)
      error("null data sent for "+oper_id+" to "+rank+" from "+my_master_rank)
    // serialize data into a byte array
    val bs = new ByteArrayOutputStream(max_buffer_size)
    val os = new ObjectOutputStream(bs)
    try {
      os.writeInt(oper_id)
      os.writeByte(tag)
      os.writeObject(data)
      os.close()
    } catch { case ex: IOException
                => serialization_error(ex) }
    val ba = bs.toByteArray
    val bb = newByteBuffer(ba.length+4)
    bb.putInt(ba.length).put(ba)
    info("    sending "+(ba.length+4)+" bytes from "+my_master_rank
         +" to "+rank+" (opr "+oper_id+")")
    try {
      master_comm.send(bb,ba.length+4,BYTE,rank,tag)
    } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  // receive the result of an operation (array blocks) from another node
  def receive_data ( rank: Int ): Any = {
    buffer.clear()
    try {
      master_comm.recv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
    } catch { case ex: MPIException
                => mpi_error(ex) }
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
    info("    received "+(len+4)+" bytes at "+my_master_rank
         +" from "+rank+" (opr "+opr_id+")")
    is.close()
    Runtime.operations(opr_id).cached = data
    Runtime.operations(opr_id).completed = true
    data
  }

  def broadcast_plan () {
    try {
      if (Communication.isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeInt(Runtime.loadBlocks.length)
        os.writeObject(Runtime.operations)
        os.writeObject(functions)
        os.close()
        val a = bs.toByteArray
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
        for ( opr_id <- Runtime.operations.indices ) {
          val x = Runtime.operations(opr_id)
          x match {
            case LoadOpr(_,b)
              if x.node != 0
              => send_data(x.node,Runtime.loadBlocks(b),opr_id,0)
            case _ => ;
          }
        }
      } else {
        val len = Array(0)
        master_comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        master_comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.length)
        val is = new ObjectInputStream(bs)
        val lb_len = is.readInt()
        Runtime.loadBlocks = Array.ofDim[Any](lb_len)
        Runtime.operations = is.readObject().asInstanceOf[Array[Opr]]
        functions = is.readObject().asInstanceOf[Array[Nothing=>Any]]
        is.close()
        for ( x <- Runtime.operations ) {
          x match {
            case LoadOpr(_,b)
              if x.node == my_master_rank
              => Runtime.loadBlocks(b) = receive_data(0)
            case _ => ;
          }
        }
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
        val a = bs.toByteArray
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        master_comm.bcast(len,1,INT,0)
        val plan_buffer = Array.fill[Byte](len(0))(0)
        master_comm.bcast(plan_buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(plan_buffer,0,plan_buffer.length)
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
                                           opr.node != my_master_rank || opr.completed })
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
                            stats.cached_blocks)
        val res = Array.fill[Int](in.length)(0)
        master_comm.reduce(in,res,in.length,INT,SUM,0)
        val max_in = Array(stats.max_cached_blocks)
        val max = Array(0)
        master_comm.reduce(max_in,max,1,INT,MAX,0)
        new Statistics(res(0),res(1),res(2),max(0))
      } catch { case ex: MPIException
                  => mpi_error(ex); stats }
}


object Communication {
  import Executor._
  var my_world_rank: Int = 0
  var i_am_master: Boolean = false
  var my_master_rank: Int = -1
  var num_of_masters: Int = 0
  var num_of_executors_per_node: Int = 1 /* must be 1 on cluster */

  def barrier () { master_comm.barrier() }

  // One master per node (can do MPI)
  def isMaster (): Boolean = i_am_master

  // Single coordinator
  def isCoordinator (): Boolean = (my_master_rank == 0)

  def mpi_startup ( args: Array[String] ) {
    try {
      // Need one MPI thread per worker node (the Master)
      InitThread(args,THREAD_FUNNELED)
      //Init(args)
      // don't abort on MPI errors
      //comm.setErrhandler(ERRORS_RETURN)
      my_world_rank = comm.getRank
      if (num_of_executors_per_node == 1)
        i_am_master = (System.getenv("OMPI_COMM_WORLD_LOCAL_RANK") == "0")
      else i_am_master = (my_world_rank < num_of_executors_per_node)  // for testing only
      // allow only masters to communicate via MPI
      master_comm = comm.split(if (isMaster()) 1 else UNDEFINED,my_world_rank)
      if (isMaster()) {
        my_master_rank = master_comm.getRank
        num_of_masters = master_comm.getSize
        val available_threads = System.getenv("OMPI_COMM_WORLD_LOCAL_SIZE")
        info("Using master "+my_master_rank+": "+getProcessorName+"/"+my_world_rank
                +" with "+available_threads+" threads and "
                +(java.lang.Runtime.getRuntime.totalMemory()/1024/1024/1024)+" GBs")
      }
    } catch { case ex: MPIException
                => System.err.println("MPI error: "+ex.getMessage)
                   ex.printStackTrace()
                   comm.abort(-1)
            }
  }

  def mpi_finalize () {
    Finalize()
  }
}

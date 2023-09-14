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

import PlanGenerator.{operations=>_,loadBlocks=>_,_}
import Runtime._
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
object operator_comparator extends Comparator[OprID] {
  def priority ( opr_id: OprID ): Int
    = operations(opr_id) match {
        case ReduceOpr(_,_,_) => 1
        case TupleOpr(_,_) => 2
        case LoadOpr(_,_) => 3
        case _ => 4
      }
  override def compare ( x: OprID, y: OprID ): Int
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
  var ready_queue = new PriorityBlockingQueue[OprID](1000,operator_comparator)
  var receive_request: Request = _
  val stats: Statistics = new Statistics()
  var exit_points: List[OprID] = Nil
  val comm: Intracomm = MPI.COMM_WORLD
  var master_comm: mpi.Comm = comm

  def mpi_error ( ex: MPIException ) {
    info("MPI error: "+ex.getMessage)
    recover(ex)
  }

  def serialization_error ( ex: IOException ) {
    System.err.println("Serialization error: "+ex.getMessage)
    ex.printStackTrace()
    comm.abort(-1)
  }

  def error ( msg: String ) {
    System.err.println("*** "+msg)
    comm.abort(-1)
  }

  var abort_count = 0
  val steps_before_abort = 10
  // kill one of the master nodes to test recovery from a fault
  def kill_master ( master: WorkerID ) {
    abort_count += 1
    if (abort_count == steps_before_abort && my_master_rank == master)
      throw new MPIException("Killing node "+master+" to test recovery")
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
        //kill_master(1)
        // prepare for the next receive (non-blocking)
        receive_request = master_comm.iRecv(buffer,buffer.capacity(),BYTE,ANY_SOURCE,ANY_TAG)
      } catch { case ex: MPIException
                  => mpi_error(ex) }
      val opr = operations(opr_id)
      if (tag == 0) {
        info("    received "+(len+4)+" bytes at "+my_master_rank+" (opr "+opr_id+")")
        cache_data(opr,data)
        enqueue_ready_operations(opr_id)
      } else if (tag == 2) {
        info("    received "+(len+4)+" bytes at "+my_master_rank)
        cache_data(opr,data)
      } else if (tag == 1) {
        opr match {
          case ReduceOpr(s,valuep,fid)  // partial reduce
            => // partial reduce copr ReduceOpr
               info("    received partial reduce result of "+(len+4)+" bytes at "
                    +my_master_rank+" (opr "+opr_id+")")
               val op = functions(fid).asInstanceOf[((Any,Any))=>Any]
               if (opr.cached == null) {
                 stats.cached_blocks += 1
                 stats.max_cached_blocks = Math.max(stats.max_cached_blocks,stats.cached_blocks)
                 opr.cached = data
               } else if (valuep)
                        opr.cached = op((opr.cached,data))
                 else { val x = opr.cached.asInstanceOf[(Any,Any)]
                        val y = data.asInstanceOf[(Any,Any)]
                        opr.cached = (x._1,op((x._2,y._2)))
                      }
               opr.reduced_count -= 1
               if (opr.reduced_count == 0) {
                 // completed ReduceOpr
                 info("    completed reduce of opr "+opr_id+" at "+my_master_rank)
                 stats.reduce_operations += 1
                 opr.completed = true
                 enqueue_ready_operations(opr_id)
               }
          case _ => ;
        }
      }
      1
    } else 0
  }

  // send the result of an operation (array blocks) to other nodes
  def send_data ( ranks: List[WorkerID], data: Any, opr_id: OprID, tag: Int ) {
    if (data == null)
      error("null data sent for "+opr_id+" to "+ranks.mkString(",")+" from "+my_master_rank)
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
    info("    sending "+(ba.length+4)+" bytes from "+my_master_rank
         +" to "+ranks.mkString(",")+" (opr "+opr_id+")")
    try {
      for ( rank <- ranks )
        master_comm.send(bb,ba.length+4,BYTE,rank,tag)
    } catch { case ex: MPIException
                => mpi_error(ex) }
  }

  // receive the result of an operation (array blocks) from another node
  def receive_data ( rank: WorkerID ): Any = {
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
    operations(opr_id).cached = data
    operations(opr_id).completed = true
    data
  }

  def broadcast_plan () {
    try {
      if (Communication.isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeInt(loadBlocks.length)
        os.writeObject(operations)
        os.writeObject(functions)
        os.close()
        val a = bs.toByteArray
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
        for ( opr_id <- operations.indices ) {
          val x = operations(opr_id)
          x match {
            case LoadOpr(_,b)
              if x.node > 0
              => send_data(List(x.node),loadBlocks(b),opr_id,0)
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
        loadBlocks = Array.ofDim[Any](lb_len)
        operations = is.readObject().asInstanceOf[Array[Opr]]
        functions = is.readObject().asInstanceOf[Array[Nothing=>Any]]
        is.close()
        for ( x <- operations ) {
          x match {
            case LoadOpr(_,b)
              if x.node == my_master_rank
              => loadBlocks(b) = receive_data(0)
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

  def broadcast ( value: Any ): Any = {
    var result = value
    try {
      if (Communication.isCoordinator()) {
        val bs = new ByteArrayOutputStream(10000)
        val os = new ObjectOutputStream(bs)
        os.writeObject(value)
        os.flush()
        os.close()
        val a = bs.toByteArray
        master_comm.bcast(Array(a.length),1,INT,0)
        master_comm.bcast(a,a.length,BYTE,0)
      } else {
        val len = Array(0)
        master_comm.bcast(len,1,INT,0)
        val buffer = Array.fill[Byte](len(0))(0)
        master_comm.bcast(buffer,len(0),BYTE,0)
        val bs = new ByteArrayInputStream(buffer,0,buffer.length)
        val is = new ObjectInputStream(bs)
        result = is.readObject()
        is.close()
      }
      master_comm.barrier()
    } catch { case ex: MPIException
                => mpi_error(ex)
              case ex: IOException
                => serialization_error(ex) }
    result
  }

  def broadcast_exit_points[I,T,S] ( e: Plan[I,T,S] ): List[OprID] = {
    exit_points = broadcast(if (Communication.isCoordinator())
                              e._3.map(x => x._2._3)
                            else null).asInstanceOf[List[OprID]]
    exit_points
  }

  // exit when the queues of all executors are empty and exit points have been computed
  def exit_poll (): Boolean = {
    try {
      val b = (ready_queue.isEmpty
               && exit_points.forall{ x => val opr = operations(x)
                                           opr.node != my_master_rank || opr.completed })
      val in = Array[Byte](if (b) 0 else 1)
      val ret = Array[Byte](0)
      master_comm.allReduce(in,ret,1,BYTE,LOR)
      ret(0) == 0
    } catch { case ex: MPIException
                => mpi_error(ex); false }
  }

  // recover from failure using lineage reconstruction
  def recover ( ex: MPIException ) {
    ex.printStackTrace()
    // TODO: needs recovery instead of abort
    comm.abort(-1)
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
  var my_world_rank: WorkerID = 0
  var i_am_master: Boolean = false
  var my_master_rank: WorkerID = -1
  var my_local_rank: WorkerID = -1
  var num_of_masters: Int = 0
  //var num_of_executors_per_node: Int = 1 /* must be 1 on cluster */

  def barrier () { master_comm.barrier() }

  // One master per node (can do MPI)
  def isMaster (): Boolean = i_am_master

  // Single coordinator
  def isCoordinator (): Boolean = (my_master_rank == 0)

  def mpi_startup ( args: Array[String] ) {
    try {
      InitThread(args,THREAD_FUNNELED)
      // don't abort on MPI errors
      comm.setErrhandler(ERRORS_RETURN)
      my_world_rank = comm.getRank
      my_local_rank = System.getenv("OMPI_COMM_WORLD_LOCAL_RANK").toInt
      // normally, a master has local rank 0 (one master per node)
      i_am_master = true // (my_local_rank < num_of_executors_per_node)
      // allow only masters to communicate via MPI
      master_comm = comm.split(if (isMaster()) 1 else UNDEFINED,my_world_rank)
      if (isMaster()) {
        my_master_rank = master_comm.getRank
        num_of_masters = master_comm.getSize
        //val available_threads = System.getenv("OMPI_COMM_WORLD_LOCAL_SIZE")
        info("Using master "+my_master_rank+": "+getProcessorName+"/"+my_world_rank
             +" with "+(java.lang.Runtime.getRuntime.availableProcessors())+" Java threads and "
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

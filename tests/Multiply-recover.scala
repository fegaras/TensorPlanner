import edu.uta.diablo._
import mpi.MPI.wtime

object Multiply {
  def main ( args: Array[String] ) {

    parami(block_dim_size,10)
    param(asynchronous,true)
    PlanGenerator.trace = true
    Executor.enable_recovery = true
    Runtime.enable_gc = false
    Runtime.enable_partial_reduce = true

    val N = args(0).toInt
    val M = args(1).toInt

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    def validate ( e: List[((Int,Int),Any)] ) {
      val A = Array.tabulate[Double] (N*M) { i => (i/M * (i%M) * 1.0) }
      val B = Array.tabulate[Double] (M*N) { i => (i/N * (i%N) * 2.0) }
      val X = Array.tabulate[Double] (N*N) { i => 0.0 }
      for { i <- 0 until N
            j <- 0 until N
            k <- 0 until M }
          X(i*N+j) += A(i*M+k)*B(k*N+j)
      for ( ((ii,jj),b) <- e ) {
        val ((n,m),_,a) = b.asInstanceOf[((Int,Int),EmptyTuple,Array[Double])]
        for ( k <- 0 until a.length ) {
          val i = ii*block_dim_size+k/m
          val j = jj*block_dim_size+k%m
          if (Math.abs((a(k) - X(i*N+j))/a(k)) > 0.01)
            println("*** "+i+" "+j+" "+a(k)+" "+X(i*N+j))
        }
      }
    }

    startup(args)

    val plan = q("""

      var Az = tensor*(N,M)[ ((i,j),i*j*1.0) | i <- 0..(N-1), j <- 0..(M-1) ];
      var Bz = tensor*(M,N)[ ((i,j),i*j*2.0) | i <- 0..(M-1), j <- 0..(N-1) ];

//Az = tensor*(N,M)[ ((i,j),m+n) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ii==i, jj==j ];
//tensor*(N,M)[ ((i,j),m+n) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ii==i, jj==j ];

      tensor*(N,N)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      """)

    var t = wtime()

    schedule(plan)
    if (isCoordinator())
      println("schedule time: %.3f secs".format(wtime()-t))

    t = wtime()
    val res = eval(plan)
    if (isCoordinator())
      println("eval time: %.3f secs".format(wtime()-t))
    val s = collect(res)
    if (isCoordinator())
      s._3.foreach(println)

    end()
  }
}

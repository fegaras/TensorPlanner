import edu.uta.diablo._
import Math._
import mpi.MPI.wtime

object Multiply {
  def main ( args: Array[String] ) {

    //parami(block_dim_size,10)
    param(asynchronous,true)
    //param(parallel,false)

    val N = args(0).toInt
    val M = args(1).toInt

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
        println("@@@ "+n+" "+m+" "+ii+" "+jj)
        for ( k <- 0 until a.length ) {
          val i = ii*block_dim_size+k/m
          val j = jj*block_dim_size+k%m
          if (a(k) != X(i*N+j))
            println("*** "+i+" "+j+" "+a(k)+" "+X(i*N+j))
        }
      }
    }

    startup(args)

    val plan = q("""

      var Az = tensor*(N,M)[ ((i,j),i*j*1.0) | i <- 0..(N-1), j <- 0..(M-1) ];
      var Bz = tensor*(M,N)[ ((i,j),i*j*2.0) | i <- 0..(M-1), j <- 0..(N-1) ];

      tensor*(N,N)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      """)

    var t = wtime()
/*
    if (isMaster()) {
      //validate
      (evalMem(plan)._3)
    }
    if (isMaster())
      println("in-memory time: "+(wtime()-t))
*/
    t = wtime()
    schedule(plan)
    if (isMaster())
      println("schedule time: "+(wtime()-t))

    t = wtime()
    val res = eval(plan)
    if (isMaster()) {
      println("eval time: "+(wtime()-t))
      collect(res).foreach(println)
    }
    //validate(res)

    end()
  }
}

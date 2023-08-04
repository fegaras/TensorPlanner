import edu.uta.diablo._
import Math._
import mpi.MPI._

object Multiply {
  def main ( args: Array[String] ) {

    param(asynchronous,true)

    startup(args)

    val plan = q("""
      var N = 2342;
      var M = 1576;

      var Az = tensor*(N,M)[ ((i,j),random()) | i <-0..(N-1), j<-0..(M-1) ];
      var Bz = Az;

      tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      """)

    var t = wtime()
    schedule(plan)
    if (isMaster())
      println("schedule time: "+(wtime()-t))

    t = wtime()
    val res = collect(eval(plan))
    if (isMaster()) {
      println("eval time: "+(wtime()-t))
      res.foreach(println)
    }

    end()

  }
}

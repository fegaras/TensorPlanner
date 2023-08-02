import edu.uta.diablo._
import Math._

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
    
    schedule(plan)

    val res = collect(eval(plan))
    if (isMaster())
      res.foreach(println)

    end()

  }
}

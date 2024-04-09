import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random
import Math._

object Factorization {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("factorization")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val repeats = args(0).toInt // how many times to repeat each experiment
    // each matrix has n*m elements
    val n = args(1).toInt
    val m = args(2).toInt
    var d = args(3).toInt;
    var iterations = args(4).toInt;
    var sparsity = 0.01;

    var alpha = 0.002;
    var beta = 0.02;

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val rand = new Random()
    def random() = rand.nextDouble()

    def testFactorization (): Double = {
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
            var R = tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() < sparsity ];
            var P = tensor*(n,d)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(d-1) ];
            var Q = tensor*(d,m)[ ((i,j),random()/n) | i <- 0..(d-1), j <- 0..(m-1) ];

            var iter = 0;
            while(iter < iterations) {
                var E1 = tensor*(n,m) (P @ Q);
                var E2 = tensor*(n,m) (R-E1);
                var P1 = tensor*(n,d)[ ((i,k),+/v) | ((i,j),e) <- E2, ((k,jj),q) <- Q, jj == j,
                                        let v = 2.0 * alpha * e * q, group by (i,k) ];
                P = tensor*(n,d)[ ((i,j), p1 + p - alpha * beta * p) | ((i,j),p1) <- P1, ((ii,jj),p) <- P, ii==i,jj==j ];
                var Q1 = tensor*(d,m)[ ((k,j),+/v) | ((i,j),e) <- E2, ((ii,k),p) <- P, ii == i,
                                        let v = 2*alpha*e*p, group by (k,j) ];
                Q = tensor*(d,m)[ ((k,j), q1 + q - alpha * beta * q) | ((k,j),q1) <- Q1, ((kk,jj),q) <- Q, jj==j,kk==k ];
                iter = iter + 1;
            }
            Q;
        """)
        val res = plan._3.count()
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def test ( name: String, f: => Double ) {
      var i = 0
      var j = 0
      var s = 0.0
      var max_time = 0.0
      while ( i < repeats && j < 10 ) {
        val t = f
        j += 1
        if (t > 0.0) {   // if f didn't crash
          s += t
          max_time = Math.max(max_time, t)
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
        }
      }
      if (i > 1) s = (s-max_time)/(i-1)
      print("*** %s n=%d, m=%d, d=%d, N=%d ".format(name,n,m,d,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("SAC Factorization",testFactorization)
  }
}

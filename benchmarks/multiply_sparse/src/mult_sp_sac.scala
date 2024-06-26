import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object Multiply {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    parami(number_of_partitions,100)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("multiply")
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    spark_context = new SparkContext(conf)
    LogManager.getRootLogger().setLevel(Level.WARN)
    param(groupByJoin,false)
    param(use_map_join,false)

    val repeats = args(0).toInt   // how many times to repeat each experiment
    // each matrix has n*m elements
    val n = args(1).toInt
    val m = args(2).toInt
    val iterations = args(3).toInt
    val sparsity = 0.01

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val rand = new Random()
    def random() = rand.nextDouble()

    // matrix multiplication of sparse-sparse
    def testMultiply (): Double = {
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
            var Az = tensor*(n)(m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1), random() < sparsity ];
            var Bz = tensor*(m)(n)[ ((i,j),random()/n) | i <- 0..(m-1), j <- 0..(n-1), random() < sparsity ];
            var iter = 0;
            while(iter < iterations) {
                Az = tensor*(n)(n)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];
                iter = iter + 1;
            }
            Az;
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
      print("*** %s n=%d m=%d N=%d ".format(name,n,m,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("SAC Multiply sparse-sparse",testMultiply)
  }
}

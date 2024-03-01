import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object LinearRegression {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("lr")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
    param(groupByJoin,true)
    //param(use_map_join,false)

    val lrate = 0.001
	  val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val reps = args(3).toInt

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val rand = new Random()
    def random() = rand.nextDouble()*10

    def testLR (): Double = {
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
          var A = tensor*(n,m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1) ];
          var C = tensor*(m)[ (i,2.3) | i <- 0..(m-1) ];
          var B = tensor*(n) (A @ C);
          var theta = tensor*(m)[ (i,random()) | i <- 0..(m-1) ];
          var iter = 0;
          while(iter < reps) {
            var B1 = tensor*(n) (A @ theta);
            B1 = B1-B;
            var d_th = tensor*(m) (A.t @ B1);
            theta = tensor*(m)[(i,th-(1.0/n)*lrate*dt) | (i,th) <- theta, (ii,dt) <- d_th, i==ii];
            iter = iter + 1;
          }
          theta;
        """)
        val res = plan._3.count()
      } catch { case x: Throwable => println(x); return -1.0 }
      (System.currentTimeMillis()-t)/1000.0
    }

    def test ( name: String, f: => Double ) {
      var i = 0
      var j = 0
      var s = 0.0
      while ( i < repeats && j < 10 ) {
        val t = f
        j += 1
        if (t > 0.0) {   // if f didn't crash
          s += t
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
        }
      }
      if (i > 1) s = s/i
      print("*** %s n=%d m=%d N=%d ".format(name,n,m,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("Diablo LR",testLR)
  }
}

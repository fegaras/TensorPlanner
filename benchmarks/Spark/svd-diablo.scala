import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object SVD {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("tiles")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
    param(groupByJoin,true)
    //param(use_map_join,false)

    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val reps = args(3).toInt
	  var sigma = 1.0
    val rand = new Random()
    def random () = (rand.nextDouble()-0.5)*10

    def testDiabloSVD(): Double = {
      val t = System.currentTimeMillis()
      val plan = q("""
        var X = tensor*(n,m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1)];
        var U = tensor*(n,1)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..0];
        var V = tensor*(m,1)[ ((i,j),random()) | i <- 0..(m-1), j <- 0..0];
        var iter = 0;
        while(iter < reps) {
          var X1 = tensor*(n,m) (U @ V.t);
          var X2 = tensor*(n,m) (X-sigma*X1);
          var covariance = tensor*(m,m) (X2.t @ X2);
          var rep = 0;
          while(rep < reps) {
            V = tensor*(m,1) (covariance @ V);
            rep = rep + 1;
          };
          var U_sigma = tensor*(n,1) (X @ V);
          //sigma = +/[a | ((i,j),a) <- U_sigma];
          U = tensor*(n,1) ((1/sigma)* U_sigma);
          iter = iter + 1;
        };
        U;
      """)
      val res = plan._3.count()
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

    test("Diablo SVD",testDiabloSVD)
  }
}

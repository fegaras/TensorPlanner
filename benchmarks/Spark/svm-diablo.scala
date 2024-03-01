import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object SVM {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("SVM")
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
	  val lrate = 0.001
	  val lambda = 1.0/reps
    val rand = new Random()
    def random () = (rand.nextDouble()-0.5)*10
	  def randomY(): Int = if(rand.nextDouble() > 0.5) 1 else -1
    def getW(w: Double, a: Double, b: Int, y: Double): Double = {
      if(y >= 1) w - 2 * lrate * lambda * w
      else w + lrate * (a * b - 2 * lambda * w)
    }

	def testDiabloSVM(): Double = {
		val t = System.currentTimeMillis()
		val plan = q("""
			var X = tensor*(n,m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1)];
			var Y = tensor*(n)[ (i,randomY()) | i <- 0..(n-1)];
			var W = tensor*(n,m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1)];
			for iter = 0, reps do {
				var y1 = tensor*(n,m) (W * X);
        var y2 = tensor*(n) [(i,+/a) | ((i,j),a) <- y1, group by i];
        var y3 = tensor*(n) (y2 * Y);
        var y4 = tensor*(n) [ (i, a1*(1-a2)) | (i,a1) <- Y, (ii,a2) <- y3, i == ii];
        var xy = tensor*(n,m) [ ((i,j),lambda*a3*b) | ((i,j),a3) <- X, (ii,b) <- y4, i==ii];
        W = tensor*(n,m) [ ((i,j),a4-lambda*lrate*a5) | ((i,j), a4) <- W, ((ii,jj),a5) <- xy, i==ii,j==jj ];
			}
			W;
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

    test("Diablo SVM",testDiabloSVM)
  }
}

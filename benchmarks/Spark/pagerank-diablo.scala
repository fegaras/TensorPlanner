import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object PageRank {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("PR")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
    param(groupByJoin,true)
    //param(use_map_join,false)

    val repeats = args(0).toInt   // how many times to repeat each experiment
    val n = args(1).toInt
    val reps = if (args.length > 2) args(2).toInt else 10
	  val b = 0.85

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val rand = new Random()
    def random() = rand.nextDouble()*10

    def testPR (): Double = {
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
          var graph = tensor*(n)(n)[ ((i,j),1.0) | i <- 0..(n-1), j <- 0..(n-1), random() > 0.1];
          var C = tensor*(n)[ (i,+/v) | ((i,j),v) <- graph, group by i ];
          var E = tensor*(n)(n)[ ((i,j), 1.0/c) | ((i,j),v) <- graph, (ii,c) <- C, ii == i ];
          var P = tensor*(n)[ (i,1.0/n) | i <- 0..n-1 ];
          var iter = 0;
          while(iter < reps) {
            P = tensor*(n) ((E.t @ (b * P)) + (1.0-b)/n);
            iter = iter + 1;
          }
          P;
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
      print("*** %s n=%d N=%d ".format(name,n,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("Diablo Pagerank",testPR)
  }
}

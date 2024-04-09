import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object PageRank {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    parami(number_of_partitions,100)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("pagerank")
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
    spark_context = new SparkContext(conf)
    param(groupByJoin,false)
    param(use_map_join,false)


    val repeats = args(0).toInt   // how many times to repeat each experiment
    val n = args(1).toInt
    val iterations = args(2).toInt

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val rand = new Random()
    def random() = rand.nextDouble()

    def testPageRank (): Double = {
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
          var b = 0.85;
          var sparsity = 0.01;

          var Gz = tensor*(n)(n)[ ((i,j),1.0) | i <-0..(n-1), j<-0..(n-1), random() < sparsity ];
          var Cz = tensor*(n) [ (i,+/v) | ((i,j),v) <- Gz, group by i ];
          var Ez = tensor*(n)(n) [ ((i,j), 1.0/c) | ((i,j),v) <- Gz, (ii,c) <- Cz, ii == i ];
          var Pz = tensor*(n)[ (i,1.0/n) | i <-0..(n-1)];

          var iter = 0;
          while(iter < iterations) {
              Pz = tensor*(n) ((Ez.t @ (b * Pz)) + (1.0-b)/n);
              iter = iter + 1;
          }
          Pz;
        """)
        val res = plan._3.count()
        plan._3.take(1).foreach(println)
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
      print("*** %s n=%d, N=%d ".format(name,n,N))
      println("tries=%d %.3f secs".format(i,s))
    }

    test("SAC PageRank",testPageRank)
  }
}

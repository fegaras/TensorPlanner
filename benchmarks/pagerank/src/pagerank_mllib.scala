import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.collection.Seq
import scala.util.Random
import Math._


object PageRank {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt   // how many times to repeat each experiment
    val n = args(1).toInt
    val sparsity = 0.01
    val iterations = args(2).toInt
    val b = 0.85
    parami(block_dim_size,1000)  // size of each dimension in a block
    val N = 1000
    parami(number_of_partitions,100)

    val conf = new SparkConf().setAppName("pagerank")
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.ERROR)
    spark_context = new SparkContext(conf)

    val G = spark_context.parallelize(for { i <- 0 until n; j <- 0 until n } yield (i,j),number_of_partitions)
              .flatMap{ case (i,j) => val rand = new Random();if(rand.nextDouble() < sparsity) List((i,j)) else None}
              .cache

    def map ( m: BlockMatrix, f: Double => Double ): BlockMatrix
      = new BlockMatrix(m.blocks.map{ case (i,a) => (i,new DenseMatrix(N,1,a.toArray.map(f))) },
              m.rowsPerBlock,m.colsPerBlock)

    def testPRMLlib(): Double = {
      var C = G.map{ case (i,j) => (i,1)}.reduceByKey(_+_)
      var matrix = G.join(C).map{ case (i,(j,c)) => ((i,j),1.0/c)}
      val blocks = matrix.map{ case ((i,j),v) => ((i/N,j/N),(i%N,j%N,v))}
                          .groupByKey()
                          .map{ case (i,l) => (i,SparseMatrix.fromCOO(N,N,l).asInstanceOf[Matrix])}
      val E = new BlockMatrix(blocks,N,N).cache
      val l = Random.shuffle((0 until (n+N-1)/N).toList)
      var P = new BlockMatrix(spark_context.parallelize(for { i <- l} 
        yield ((i,0),new DenseMatrix(N,1,Array.tabulate(N){ j => 1.0/n }))),N,1).cache
      var k = 0
      val t = System.currentTimeMillis()
      while (k < iterations) {
        k += 1
        var Q = map(P, b*_).cache
        P = map(E.transpose.multiply(Q),_+(1-b)/n).cache
      }
      println(P.blocks.count)
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
      if (i > 0) s = (s-max_time)/(i-1)
      print("*** %s n=%d, N=%d ".format(name,n,N))
      println("tries=%d %.3f secs".format(i,s))
    }
 
    test("MLlib PageRank",testPRMLlib)
    spark_context.stop()
  }
}

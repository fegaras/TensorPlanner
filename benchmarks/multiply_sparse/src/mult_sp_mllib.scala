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

object Multiply {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt   // how many times to repeat each experiment
    // each matrix has n*m elements
    val n = args(1).toInt
    val m = args(2).toInt
    val iterations = args(3).toInt
    val sparsity = 0.01
    parami(block_dim_size,1000)  // size of each dimension in a block
    val N = 1000
    parami(number_of_partitions,100)

    val conf = new SparkConf().setAppName("multiply")
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    spark_context = new SparkContext(conf)
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()
    def random() = rand.nextDouble()

    def randomTileSparse ( nd: Int, md: Int ): SparseMatrix = {
      var entries = scala.collection.mutable.ArrayBuffer[(Int,Int,Double)]()
      for (i <- 0 to nd-1; j <- 0 to md-1) {
        if (random() < sparsity)
          entries += ((i,j,random()))
      }
      SparseMatrix.fromCOO(nd,md,entries)
    }

    def randomSparseMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+N-1)/N).toList)
      val r = Random.shuffle((0 until (cols+N-1)/N).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
            .map{ case (i,j) => ((i,j),randomTileSparse(if ((i+1)*N > rows) rows%N else N,
                              if ((j+1)*N > cols) cols%N else N)) }
    }

    val Am = randomSparseMatrix(n,m).cache()
    val Bm = randomSparseMatrix(m,n).cache()

    var A = new BlockMatrix(Am,N,N).cache
    var B = new BlockMatrix(Bm,N,N).cache

    def map ( m: BlockMatrix, f: Double => Double ): BlockMatrix
      = new BlockMatrix(m.blocks.map{ case (i,a) => (i,new DenseMatrix(N,N,a.toArray.map(f))) },
              m.rowsPerBlock,m.colsPerBlock)

    // matrix multiplication of sparse-sparse tiled matrices in MLlib.linalg
    def testMultiplyMLlib(): Double = {
      val t = System.currentTimeMillis()
      try {
      for(iter <- 0 to iterations-1)
        A = A.multiply(B)
      val x = A.blocks.count
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
 
    test("MLlib Multiply sparse-sparse",testMultiplyMLlib)
    spark_context.stop()
  }
}

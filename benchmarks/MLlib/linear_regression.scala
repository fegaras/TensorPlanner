import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
//import com.github.fommil.netlib.NativeSystemBLAS
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.collection.Seq
import scala.util.Random
import Math._

object LinearRegression extends Serializable {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
   }

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("linear_regression")
    spark_context = new SparkContext(conf)
	val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
	
    parami(number_of_partitions,10)
    parami(block_dim_size,1000)
	//parami(broadcast_limit, 10000)
    val repeats = args(0).toInt
    val N = 1000
	val validate = false
	
    val lrate = 0.001
    val total_size = args(1).toInt
    val n = (0.8*total_size).toInt
    val test_size = total_size-n
    val m = 3000
    val numIter = 10
    val rand = new Random()

    val X_train = spark_context.textFile(args(2),number_of_partitions)
              .map( line => { val a = line.split(",").toList
              				((a(0).toInt,a(1).toInt),a(2).toDouble)} ).cache
    val y_train = spark_context.textFile(args(3),number_of_partitions)
              .map( line => { val a = line.split(",").toList
                             (a(0).toInt,a(1).toDouble)} ).cache
	val X_test = spark_context.textFile(args(4),number_of_partitions)
              .map( line => { val a = line.split(",").toList
              				((a(0).toInt,a(1).toInt),a(2).toDouble)} ).cache
    val y_test = spark_context.textFile(args(5),number_of_partitions)
              .map( line => { val a = line.split(",").toList
                             (a(0).toInt,a(1).toDouble)} ).cache

	def randomTile ( nd: Int, md: Int ): DenseMatrix = {
		val max = 10
		val rand = new Random()
		new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
	}

	def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),Matrix)] = {
		val l = Random.shuffle((0 until (rows+N-1)/N).toList)
		val r = Random.shuffle((0 until (cols+N-1)/N).toList)
		spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
					.map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
															if ((j+1)*N > cols) cols%N else N)) }
	}

	val Xm = randomMatrix(n,m).cache()
	val Ym = randomMatrix(n,1).cache()

	// forces df to materialize in memory and evaluate all transformations
	// (noop write format doesn't have much overhead)
	def force ( df: DataFrame ) {
		df.write.mode("overwrite").format("noop").save()
	}

	def vect ( a: Iterable[Double] ): org.apache.spark.ml.linalg.Vector = {
		val s = Array.ofDim[Double](m)
		var count = 0
		for(x <- a) {
		s(count) = x
		count += 1
		}
		Vectors.dense(s)
	}

	// Create dataframes from data
	X_train.map{case ((i,j),v) => (i,v)}.groupByKey()
			.map{case (i,v) => (i, vect(v))}.toDF.createOrReplaceTempView("X_d")
	y_train.toDF.createOrReplaceTempView("Y_d")
	X_test.map{case ((i,j),v) => (i,v)}.groupByKey()
			.map{case (i,v) => (i, vect(v))}.toDF.createOrReplaceTempView("X_test_d")
	y_test.toDF.createOrReplaceTempView("Y_test_d")
	// Load training data
	val training_data = spark.sql("select Y_d._2 as label, X_d._2 as features from X_d join Y_d on X_d._1=Y_d._1")
		.rdd.map{row => LabeledPoint(
		row.getAs[Double]("label"),
		row.getAs[org.apache.spark.ml.linalg.Vector]("features")
	)}.toDF.cache()
	force(training_data)
	training_data.show(false)

    def testMLlibLR(): Double = {
		val t = System.currentTimeMillis()
		val lr = new LinearRegression().setMaxIter(numIter).setRegParam(0.3).setElasticNetParam(0.8)

        val lrModel = lr.fit(training_data)
		println(lrModel.coefficients.size)
		// Summarize the model over the training set and print out some metrics
		val trainingSummary = lrModel.summary
		println(s"numIterations: ${trainingSummary.totalIterations}")
		println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
		//trainingSummary.residuals.show()
		println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
		println(s"r2: ${trainingSummary.r2}")
		force(training_data)
		if(validate) {
			// Load training data
			val test_data = spark.sql("select Y_test_d._2 as label, X_test_d._2 as features from X_test_d join Y_test_d on X_test_d._1=Y_test_d._1")
				.rdd.map{row => LabeledPoint(
				row.getAs[Double]("label"),
				row.getAs[org.apache.spark.ml.linalg.Vector]("features")
			)}.toDF.cache()
			force(test_data)
			val predictions = lrModel.transform(test_data)
			force(training_data)
			predictions.rdd.count()
			println(lrModel.evaluate(test_data).meanSquaredError)
			force(training_data)
		}
		(System.currentTimeMillis()-t)/1000.0
    }

	def convertMatrix(mat1: Matrix, f:Double=>Double): Matrix = {
		var arr = Array[Double]()
		var size = 0
		for(v <- mat1.colIter; i <- 0 to v.size-1) {
			arr = arr :+ f(v(i))
			size += 1
		}
		Matrices.dense(size, 1, arr)
	}

	def testMLlibHandWrittenLR(): Double = {
		var theta = new CoordinateMatrix(spark_context.parallelize(0 to m-1).map(i => MatrixEntry(i,0,rand.nextDouble()-0.5))).toBlockMatrix(N,1).cache
		val input1 = new CoordinateMatrix(X_train.map{ case ((i,j),v) => MatrixEntry(i,j,v)}).toBlockMatrix(N,N).cache
		val output1 = new CoordinateMatrix(y_train.map{ case (i,v) => MatrixEntry(i,0,v)}).toBlockMatrix(N,1).cache
		val t = System.currentTimeMillis()
		for(itr <- 0 until numIter) {
			val x_theta_minus_y = input1.multiply(theta).subtract(output1)
			var d_theta = input1.transpose.multiply(x_theta_minus_y)
			val d_theta_blocks = d_theta.blocks.map{ case ((i,j),v) => ((i,j),convertMatrix(v,_*lrate*(1.0/n)))}
			d_theta = new BlockMatrix(d_theta_blocks, N, 1)
			theta = theta.subtract(d_theta)
			theta.cache
		}
		theta.blocks.count
		if(validate) {
			val input2 = new CoordinateMatrix(X_test.map{ case ((i,j),v) => MatrixEntry(i,j,v)}).toBlockMatrix(N,N).cache
			val output2 = new CoordinateMatrix(y_test.map{ case (i,v) => MatrixEntry(i,0,v)}).toBlockMatrix(N,1).cache
			val x_theta_minus_y = input2.multiply(theta).subtract(output2)
			val cost = x_theta_minus_y.toCoordinateMatrix().entries
							.map(e => (0.5/test_size)*e.value*e.value).reduce(_+_)
			println("Cost: "+cost)
		}
		(System.currentTimeMillis()-t)/1000.0
    }

	def test ( name: String, f: => Double ) {
		val cores = Runtime.getRuntime().availableProcessors()
		var i = 0
		var j = 0
		var s = 0.0
		while ( i < repeats && j < 10 ) {
			val t = f
			j += 1
			if (t > 0.0) {   // if f didn't crash
			  i += 1
			  println("Try: "+i+"/"+j+" time: "+t)
			  if(i > 1) s += t
			}
		}
		if (i > 0) s = s/(i-1)
		print("*** %s cores=%d n=%d m=%d N=%d ".format(name,cores,total_size,m,N))
		println("tries=%d %.3f secs".format(i,s))
    }

    test("MLlib Linear Regression",testMLlibLR)
	test("MLlib Handwritten Linear Regression",testMLlibHandWrittenLR)

    spark_context.stop()
  }
}

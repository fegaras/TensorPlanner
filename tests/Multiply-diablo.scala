import edu.uta.diablo._
import org.apache.spark._
import System._

object MultiplyDiablo {
  def main ( args: Array[String] ) {
    val N = args(0).toInt
    val M = args(1).toInt

    val conf = new SparkConf().setAppName("tiles")
    spark_context = new SparkContext(conf)
    //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired","true")

    param(groupByJoin,true)

    val t = currentTimeMillis()

    val X = q("""

      var Az = tensor*(N,M)[ ((i,j),i*j*1.0) | i <- 0..(N-1), j <- 0..(M-1) ];
      var Bz = tensor*(M,N)[ ((i,j),i*j*2.0) | i <- 0..(M-1), j <- 0..(N-1) ];

      tensor*(N,N)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      """)

     X._3.count()

      println("diablo time: "+(currentTimeMillis()-t)/1000.0)
  }
}

import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    spark_context = new SparkContext(conf)

    q("""
        var M: matrix[Double] = tensor*(100,100)[ ((i,j),i*100.0+j) | i <- 0..99, j <- 0..99 ];
        var N = M;

                  for i = 0, 99 do {
//                     N[i,0] = 0.0;
                     for j = 0, 99 do
                        N[i,0] += M[i,j];
                  };
        N
    """)

  }
}

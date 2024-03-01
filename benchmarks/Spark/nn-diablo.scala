import edu.uta.diablo._
import org.apache.spark._
import org.apache.log4j._
import scala.util.Random

object NeuralNetwork {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = block_dim_size

    val conf = new SparkConf().setAppName("NN")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)
    param(groupByJoin,true)
    //param(use_map_join,false)

    val lrate = 0.5
		val repeats = args(0).toInt
		val n = args(1).toInt
		val m = args(2).toInt
		val reps = args(3).toInt

		val nn_architecture = List((m, 32), (32, 8), (8, 1))

		val rand = new Random()
		def random(): Double = (rand.nextDouble()-0.5)
		
		def getMax(z: Double) = math.max(0.0,z)
		def getExp(z: Double) = math.exp(z)
		def getVal(z: Double, a: Double) = if(z <= 0.0) 0.0 else a
		def getSigmoid(z: Double) = 1/(1+math.exp(-z))

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    def testNN (): Double = {
      val nl = nn_architecture.size
			var l = 0
      var t = System.currentTimeMillis()
      try {
        val plan = q("""
          var X_d = tensor*(n,m)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(m-1) ];
		      var Y_d = tensor*(m)[ (i,5.0*random()) | i <- 0..(m-1) ];
          var w_arr = tensor*(nl,m,m) [ ((i,j,k),random()) | i <- 0..(nl-1), j <- 0..(m-1), k <- 0..(m-1)];
          var Z_arr = tensor*(nl,n,m) [ ((i,j,k),random()) | i <- 0..(nl-1), j <- 0..(n-1), k <- 0..(m-1)];
          var A_arr = tensor*(nl,n,m) [ ((i,j,k),random()) | i <- 0..(nl-1), j <- 0..(n-1), k <- 0..(m-1)];
          var iter = 0;
          while(iter < reps) {
            iter = iter + 1;
            var A_curr1 = X_d;
            for l = 0, nl-1 do {
              A_arr = tensor*(nl,n,m)[ ((kk,i,j),if(kk==l) v else v1) | ((i,j),v) <- A_curr1, ((kk,ii,jj),v1) <- A_arr, i==ii, j==jj];
              var Z_curr1 = tensor*(n,m)[ ((i,j),+/v) | ((i,k),a) <- A_curr1, ((ii,kk,j),w) <- w_arr,
                          kk == k, ii==l, let v = w*a, group by (i,j) ];
              if(l < nl-1) A_curr1 = tensor*(n,m)[ ((i,j),getMax(z)) | ((i,j),z) <- Z_curr1 ];
              else A_curr1 = tensor*(n,m)[ ((i,j),1/(1+getExp(-z))) | ((i,j),z) <- Z_curr1 ];
              Z_arr = tensor*(nl,n,m)[ ((kk,i,j),if(kk==l) v else v1) | ((i,j),v) <- Z_curr1, ((kk,ii,jj),v1) <- Z_arr,i==ii,j==jj ];
            }
            var dA_prev = tensor*(n,m)[ ((i,jj),(2.0/n)*(y_hat-y)) | (i,y) <- Y_d, ((ii,jj),y_hat) <- A_curr1,
                            i == ii, jj == 0 ];
            for l = 0, nl-1 do {
              var dA_curr = dA_prev;

              var Z_curr2 = tensor*(n,m)[ ((i,j),v) | ((kk,i,j),v) <- Z_arr, kk==nl-1-l ];
              var A_curr2 = tensor*(n,m)[ ((i,j),v) | ((kk,i,j),v) <- A_arr, kk==nl-1-l ];
              var w2 = tensor*(m,m)[ ((i,j),w) | ((kk,i,j),w) <- w_arr, kk==nl-1-l ];
              var dZ_curr = tensor*(n,m)[ ((i,j), v) | ((i,j),a) <- dA_curr, ((ii,jj),z) <- Z_curr2,
                          i == ii, j == jj, let v = getVal(z,a) ];
              if(l == 0) dZ_curr = tensor*(n,m)[ ((i,j), dA*sig*(1-sig)) | ((i,j),dA) <- dA_curr, ((ii,jj),z) <- Z_curr2,
                          i == ii, j == jj, let sig = 1/(1+getExp(-z)) ];
              var dW_curr = tensor*(m,m)[ ((j,jj), +/v) | ((i,j),a) <- A_curr2, ((ii,jj),z) <- dZ_curr,
                          i == ii, let v = z*a, group by (j,jj) ];
              dA_prev = tensor*(n,m)[ ((i,ii),+/v) | ((i,j),z) <- dZ_curr, ((ii,jj),w) <- w2,
                          j == jj, let v = w*z, group by (i,ii) ];
              var w_update = tensor*(m,m)[((i,j),w-lrate*dw) | ((i,j),w) <- w2, ((ii,jj),dw) <- dW_curr,
                          i==ii, j==jj ];
              w_arr = tensor*(nl,m,m)[ ((kk,i,j),if(kk==nl-1-l) v else v1) | ((i,j),v) <- w_update, ((kk,ii,jj),v1)<-w_arr,i==ii,j==jj ];
            }
          }
          w_arr;
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

    test("Diablo Neural Network",testNN)
  }
}

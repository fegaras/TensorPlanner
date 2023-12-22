import edu.uta.diablo._
import scala.util.Random

object Filter {
  def main ( args: Array[String] ) {
    parami(block_dim_size,10)
    val N = 10
    param(asynchronous,true)
    PlanGenerator.trace = false

    val n = args(0).toInt
    val repeats = 1
    startup(args)

    val rand = new Random()
    def random() = rand.nextDouble()*10

    // matrix multiplication of dense-dense
    def testFilter (): Double = {
      val t = System.currentTimeMillis()
      var idx = 0
      try {
        val plan = q("""
            var Az = tensor*(n,n,n)[ ((i,j,k),i*j*1.0) | i <- 0..(n-1), j <- 0..(n-1), k <- 0..(n-1) ];
            var Bz = tensor*(n,n) [ ((i,j),v) | ((i,j,k),v) <- Az, k==idx];
            Bz;
        """)
        schedule(plan)
        val res = eval(plan)
        if (isCoordinator())
          println("eval time: %.3f secs".format((System.currentTimeMillis()-t)/1000.0))
        val s = collect(res)
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
          if(i > 0) s += t
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
        }
      }
      if (i > 0) s = s/(i-1)
      if (isCoordinator()) {
        print("*** %s n=%d m=%d N=%d ".format(name,n,n,N))
        println("tries=%d %.3f secs".format(i,s))
      }
    }

    test("TensorPlanner Filter dense-dense",testFilter)
    end()
  }
}

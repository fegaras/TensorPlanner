import edu.uta.diablo._
import scala.util.Random

object Multiply {
  def main ( args: Array[String] ) {
    parami(block_dim_size,1000)
    val N = 1000
    param(asynchronous,true)
    PlanGenerator.trace = false

    val repeats = args(0).toInt   // how many times to repeat each experiment
    // each matrix has n*n elements
    val n = args(1).toInt
    val reps = if (args.length > 2) args(2).toInt else 10
    startup(args)

    val rand = new Random()
    def random() = rand.nextDouble()*10

    // matrix multiplication of dense-dense
    def testMultiply (): Double = {
      val t = System.currentTimeMillis()
      PlanGenerator.operations.clear()
      PlanGenerator.functions.clear()
      PlanGenerator.loadBlocks.clear()
      var is_A = true
      try {
        val plan = q("""
            var Az = tensor*(n,n)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(n-1) ];
            var Bz = tensor*(n,n)[ ((i,j),random()) | i <- 0..(n-1), j <- 0..(n-1) ];
            var Cz = Az;
            var iter = 0;
            while(iter < reps) {
                Cz = tensor*(n,n)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];
                if(is_A) {
                    Az = Cz;
                }
                else {
                    Bz = Cz;
                }
                is_A = !is_A;
                iter = iter + 1;
            }
            Cz;
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
        print("*** %s n=%d m=%d N=%d ".format(name,n,m,N))
        println("tries=%d %.3f secs".format(i,s))
      }
    }

    test("TensorPlanner Multiply dense-dense",testMultiply)
    end()
  }
}

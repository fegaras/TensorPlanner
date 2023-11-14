import edu.uta.diablo._
import mpi.MPI.wtime


object Multiply {
  def main ( args: Array[String] ) {
    //parami(block_dim_size,10)
    val N = block_dim_size
    param(asynchronous,true)
    PlanGenerator.trace = true
    //Runtime.enable_partial_reduce = false

    val repeats = args(0).toInt   // how many times to repeat each experiment
    // each matrix has n*m elements
    val n = args(1).toInt
    val m = n
    val reps = if (args.length > 2) args(2).toInt else 10
    startup(args)

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val t = System.currentTimeMillis()

    val plan = q("""
            var Az = tensor*(n,n)[ ((i,j),i*j*1.0) | i <- 0..(n-1), j <- 0..(n-1) ];
            var Bz = tensor*(n,n)[ ((i,j),i*j*2.0) | i <- 0..(n-1), j <- 0..(n-1) ];
            var Cz = tensor*(n,n)[ ((i,j),i*j*2.0) | i <- 0..(n-1), j <- 0..(n-1) ];
            var t = wtime();
            var iter = 0;
            while(iter < reps) {
              Az = tensor*(n,n)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];
              iter = iter + 1;
            }
            Az;
    """)

    if (isCoordinator())
      println("plan creation: %.3f secs".format((System.currentTimeMillis()-t)/1000.0));

    schedule(plan)

    // matrix multiplication of dense-dense
    def testMultiply (): Double = {
      var t = wtime()
      try {
        Runtime.operations.foreach{ e => Runtime.initialize_opr(e); e.status = PlanGenerator.notReady }
        val res = eval(plan)
        t = wtime()-t
        val s = collect(res)
      } catch { case x: Throwable => println(x); return -1.0 }
      t
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
          if (isCoordinator())
            println("Try: "+i+"/"+j+" time: "+t)
        }
      }
      if (i > 1) s = s/i
      if (isCoordinator()) {
        print("*** %s n=%d m=%d N=%d ".format(name,n,m,N))
        println("tries=%d %.3f secs".format(i,s))
      }
    }

    test("TensorPlanner Multiply dense-dense",testMultiply)
    end()
  }
}

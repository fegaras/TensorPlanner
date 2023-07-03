import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Math._

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    spark_context = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    parami(number_of_partitions,10)
    parami(block_dim_size,10)

    val N = args(0).toInt
    val NN = args(1).toInt
    val sparsity = args(2).toDouble

    def eq ( x: ((Int,Int),EmptyTuple,Array[Double]), y: ((Int,Int),EmptyTuple,Array[Double]) ) {
      if (x._1._1 != y._1._1 || x._1._2 != y._1._2)
        println("Wrong sizes: "+Tuple2(x._1._1,x._1._2)+Tuple2(y._1._1,y._1._2))
      else {
        for { i <- 0.until(x._1._1); j <- 0.until(x._1._2) }
          if (Math.abs(x._3(i*x._1._2+j) - y._3(i*x._1._2+j)) > 0.01)
            println(s"($i,$j) (${x._3(i*x._1._2+j)},${y._3(i*x._1._2+j)})")
      }
    }

    def pr ( s: ((Int,Int),((Int,Int),EmptyTuple,Array[Double])) ) {
      val x = s._2
      println("*** block: "+s._1+"  "+x._1._1+"*"+x._1._2)
      for { i <- 0.until(x._1._1); j <- 0.until(x._1._2) } {
        if (i*x._1._2+j >= x._3.length)
          println("error "+i+" "+j)
        if (x._3(i*x._1._2+j) > 0)
          println("> "+i+" "+j+" "+x._3(i*x._1._2+j))
      }
    }

    def prs ( x: (Int,Int,(Array[Int],Array[Int],Array[Double])) ) {
      println("*** size: "+x._1+" "+x._2)
      println("** "+x._3._1.toList)
      println("** "+x._3._2.toList)
      println("** "+x._3._3.toList)
    }

    type tiled_matrix = ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))])

    type sparse_matrix = (Int,Int,RDD[((Int,Int),(Int,Int,(Array[Int],Array[Int],Array[Double])))])

    val count = spark_context.longAccumulator("acc")

    def gbj2 ( A: tiled_matrix, B: tiled_matrix ): ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))]) = {
      val BN = 10
      val nn = Math.ceil(N*1.0/BN).toInt
      ((N,N),EmptyTuple(),
       A._3.flatMap{ case ((i,k),a) => (0 to nn).map(j => ((i,j),(k,a))) }
         .cogroup( B._3.flatMap{ case ((kk,j),b) => (0 to nn).map(i => ((i,j),(kk,b))) } )
         .flatMap { case ((ci,cj),(as,bs))
                    => if (as.isEmpty || bs.isEmpty)
                         Nil
                       else {
                         var ns = 0; var ms = 0
                         val c = Array.ofDim[Double](BN*BN)
                         for { (k1,((na,ma),_,a)) <- as
                               (k2,((nb,mb),_,b)) <- bs if k2 == k1 } {
                           ns = na
                           ms = mb
                           for { i <- (0 until na).par } {
                             var k = 0
                             while (k<ma && k<nb) {
                               var j = 0
                               while (j<mb) {
                                 c(i*ms+j) += a(i*ma+k)*b(k*mb+j)
                                 j += 1
                               }
                               k += 1
                             }
                           }
                         }
                         count.add(1)
                         List(((ci,cj),((ns,ms),EmptyTuple(),Array.tabulate(ns*ms){ i => c(i) })))
                       }
                    })
    }

    def gbj ( A: tiled_matrix, B: tiled_matrix ): ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))]) = {
println("@@@ "+block_dim_size+" "+number_of_partitions)
      val BN = block_dim_size
      val grid_dim = Math.sqrt(number_of_partitions).toInt
      val grid_blocks = Math.max(1,Math.ceil(N*1.0/BN/grid_dim).toInt)
println("@@@ "+grid_dim+" "+grid_blocks)
      ((N,N),EmptyTuple(),
       A._3.flatMap{ case ((ci,ck),ta) => (0 until grid_dim).map(cj => ((ci%grid_dim,cj),((ci,ck),ta))) }
         .cogroup( B._3.flatMap{ case ((ckk,cj),tb) => (0 until grid_dim).map(ci => ((ci,cj%grid_dim),((ckk,cj),tb))) } )
         .flatMap { case (_,(as,bs))
                      => groupByJoin_mapper(as,bs,grid_dim,grid_blocks,
                             (A: ((Int,Int),EmptyTuple,Array[Double]),
                              B: ((Int,Int),EmptyTuple,Array[Double]))
                               => { val n = A._1._1
                                    val m = B._1._2
                                    count.add(1)
                                    q("tensor(n,m)[ ((i,j),+/c) | ((i,k),a) <- A, ((kk,j),b) <- B, k == kk, let c = a*b, group by (i,j) ]")
                                  },
                             (X: ((Int,Int),EmptyTuple,Array[Double]), P: ((Int,Int),EmptyTuple,Array[Double]))
                               => { val n = X._1._1
                                    val m = P._1._2
                                    q("tensor(n,m)[ ((i,j),x+p) | ((i,j),x) <- X, ((ii,jj),p) <- P, ii==i, jj==j ]")
                                  })
                  })
    }

    @inline
    def gbjs ( A: sparse_matrix, B: sparse_matrix ): ((Int,Int),EmptyTuple,RDD[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))]) = {
println("@@@ "+block_dim_size+" "+number_of_partitions)
      val BN = block_dim_size
      val grid_dim = Math.sqrt(number_of_partitions).toInt
      val grid_blocks = Math.max(1,Math.ceil(N*1.0/BN/grid_dim).toInt)
println("@@@ "+grid_dim+" "+grid_blocks)
      ((N,N),EmptyTuple(),
       A._3.flatMap{ case ((ci,ck),ta) => (0 until grid_dim).map(cj => ((ci%grid_dim,cj),((ci,ck),ta))) }
         .cogroup( B._3.flatMap{ case ((ckk,cj),tb) => (0 until grid_dim).map(ci => ((ci,cj%grid_dim),((ckk,cj),tb))) } )
         .flatMap { case (_,(as,bs))
                      => groupByJoin_mapper(as,bs,grid_dim,grid_blocks,
                             (A: (Int,Int,(Array[Int],Array[Int],Array[Double])),
                              B: (Int,Int,(Array[Int],Array[Int],Array[Double])))
                               => { val n = A._1
                                    val m = B._2
                                    count.add(1)
                                    q("tensor(n,m)[ ((i,j),+/c) | ((i,k),a) <- A, ((kk,j),b) <- B, k == kk, let c = a*b, group by (i,j) ]")
                                  },
                             (X: ((Int,Int),EmptyTuple,Array[Double]), P: ((Int,Int),EmptyTuple,Array[Double]))
                               => { val n = X._1._1
                                    val m = P._1._2
                                    q("tensor(n,m)[ ((i,j),x+p) | ((i,j),x) <- X, ((ii,jj),p) <- P, ii==i, jj==j ]")
                                  })
                  })
    }

    //param(parallel,false)
    //param(data_frames,true)
    param(groupByJoin,true)

      val M = q("tensor(N,NN)[ ((i,j),if (random()>sparsity) 0.0 else random()*100) | i <- 0..N-1, j <- 0..NN-1 ]")
      val MM = q("tensor(NN,N)[ ((i,j),if (random()>sparsity) 0.0 else random()*100) | i <- 0..NN-1, j <- 0..N-1 ]")

      val E = q("tensor*(N)(NN)[ ((i,j),M[i,j]) | i <- 0..N-1, j <- 0..NN-1 ]")
      val EE = q("tensor*(NN)(N)[ ((i,j),MM[i,j]) | i <- 0..NN-1, j <- 0..N-1 ]")

      //val R = q("tensor*(N,N)[ ((i,j),+/c) | ((i,k),a) <- E, ((kk,j),b) <- EE, k == kk, let c = a*b, group by (i,j) ]")

      //println("!!! "+q("tensor(N,N)[ ((i,j),+/c) | ((i,k),a) <- M, ((kk,j),b) <- MM, k == kk, let c = a*b, group by (i,j) ]")._3.toList)

      //println("@@@ "+M._3.toList)
      //R._3.collect.toList.map(x => pr(x))
      //println("++++")
      val RR = gbjs(E,EE)
      RR._3.collect.toList.foreach(x => {println("# "+x._1);pr(x)})
      //eq(M,q("tensor(N,NN)[ ((i,j),n) | ((i,j),n) <- E ]"))
count.reset()
      eq(q("tensor(N,N)[ ((i,j),n) | ((i,j),n) <- RR ]"),
         q("tensor(N,N)[ ((i,j),+/c) | ((i,k),a) <- M, ((kk,j),b) <- MM, k == kk, let c = a*b, group by (i,j) ]"))

      println("^^^ "+(Math.ceil(N/10.0).toInt)+" "+(Math.ceil(NN/10.0).toInt)+" "+count.value)

  }
}

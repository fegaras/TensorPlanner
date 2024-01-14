import edu.uta.diablo._

object Test {
  def main ( args: Array[String] ) {

    parami(block_dim_size,10)
    param(asynchronous,true)
    PlanGenerator.trace = true
    //Runtime.enable_gc = true
    //Runtime.enable_partial_reduce = false

    startup(args)

    def pr ( a: ((Int,Int),EmptyTuple,List[((Int,Int),((Int,Int),EmptyTuple,Array[Double]))]) ) {
      val bn = block_dim_size
      val n = a._1._1
      val m = a._1._2
      val z = Array.ofDim[Double](n*m)
      for { ((ci,cj),((cn,cm),_,s)) <- a._3;
            i <- 0 until cn;
            j <- 0 until cm } {
         z((ci*bn+i)*m + (cj*bn+j)) = s(i*cm+j)
      }
      for ( i <- 0 until n ) {
         for ( j <- 0 until m )
            print("\t%3.1f".format(z(i*m+j)))
         println()
      }
    }

    val N = 23
    val M = 15

    val graph = textFile("graph.txt").map{ case (_,line) => val a = line.split(","); (a(0).toInt,a(0).toInt) }

    val plan = q("""

      var Az = tensor*(N,M)[ ((i,j),i*j*1.0) | i <-0..(N-1), j<-0..(M-1) ];
      var Bz = tensor*(M,N)[ ((i,j),3.4) | i <-0..(N-1), j<-0..(M-1) ];
      var Cz = tensor*(N,M)[ ((i,j),4.5) | i <-0..(N-1), j<-0..(M-1) ];
      //var V = tensor*(N)[ (i,2.3) | i <-0..(N-1) ];

      //tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];


      //Az = Az+Bz-3.5*Cz;
      //Az

      tensor*(N)[ (i,+/v) | (i,v) <- V, ((ii,j),a) <- Az, ii==i, let v = a+v, group by i ];

      //tensor*(N,M)[ ((i,j),v+a) | (i,v) <- V, ((ii,j),a) <- Az, ii==i ];

      //tensor*(N,M)[ ((i,j),a+1) | ((i,j),a) <- Az ];

      //tensor*(N)[ (i,*/a) | ((i,j),a) <- Az, group by i ];

      //tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N)(M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N,M)[ ((i,j),m+n) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ii==i, jj==j ];

      //tensor*(N,M)[ ((i,j),m+n+k) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ((iii,jjj),k) <- Cz, ii==i, jj==j, iii==i, jjj==j ];

      //tensor*(N,M)[ ((i,j),+/v) | ((i,k),a) <= Az, ((kk,l),b) <= Bz, ((ll,j),c) <- Cz, kk==k, ll==l, let v = a*b*c, group by (i,j) ];

      //tensor*(N,M)[ (((i+1)%N,j),a) | ((i,j),a) <- Az ];

      //tensor*(N-12,M-11)[ (((N-12+i)%N,(M-11+j)%M),a) | ((i,j),a) <- Az ];    // A[12:22,11:14]

      //tensor*(8,3)[ (((N-12+i)%N,(M-11+j)%M),a) | ((i,j),a) <- Az ];    // A[12:19,11:13]

      //Az[12:19,5:11:2];

      // error: tensor*(N)[ (i,+/v) | (i,v) <- V, ((ii,j),a) <- Az, ii==i, let v = a+v, group by i ];

      // error: tensor*(N,M)[ ((i-12,j-14),a) | ((i,j),a) <- Az, i>=12, j>=14 ];

      //var n = +/[ a | ((i,j),a) <- Az ];
      //println(n);
      //Az

/*
        for i = 0, N-1 do
            for j = 0, M-1 do
               Cz[i,j] = Az[i,j]+Bz[i,j];

        for i = 0, N-1 do
            for j = 0, M-1 do {
               Cz[i,j] = 0.0;
               for k = 0, N-1 do
                  Cz[i,j] += Az[i,k]*Bz[k,j];
            };
        Cz

      for i = 0, 20 do
         Az = tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];
      Az

     tensor*(100)[ (i,j.length) | (i,j) <- graph, group by i ];
*/

      """)

    if (false && isCoordinator())
      evalMem(plan)

    schedule(plan)

    if (isCoordinator())
      PlanGenerator.print_plan(plan)

    val res = collect(eval(plan))
    if (isCoordinator()) {
      val s = res.asInstanceOf[((Int,Int),EmptyTuple,List[((Int,Int),((Int,Int),
                        EmptyTuple,Array[Double]))])]
      println(s)
      //println(s._3.map(x => x._2._3.length))
     // pr(s)
    }

    end()

  }
}

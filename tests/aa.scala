import edu.uta.diablo._

object Test {
  def main ( args: Array[String] ) {

    parami(block_dim_size,10)
    param(asynchronous,true)
    PlanGenerator.trace = true
    //Runtime.enable_gc = false

    startup(args)

    def pr ( x: (Any,Any) ) {
      val z = x._2.asInstanceOf[(Any,Any,Array[Double])]
      println(x+"   "+z._3.map(w => "%.1f".format(w)).toList)
    }

    val N = 13
    val M = 25

    val plan = q("""

      var Az = tensor*(N,M)[ ((i,j),2.3) | i <-0..(N-1), j<-0..(M-1) ];
      var Bz = tensor*(N,M)[ ((i,j),3.4) | i <-0..(N-1), j<-0..(M-1) ];
      var Cz = tensor*(N,M)[ ((i,j),4.5) | i <-0..(N-1), j<-0..(M-1) ];

      //tensor*(N,M)[ ((i,j),a+1) | ((i,j),a) <- Az ];

      //tensor*(N)[ (i,*/a) | ((i,j),a) <- Az, group by i ];

      tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N)(M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N,M)[ ((i,j),m+n) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ii==i, jj==j ];

      //tensor*(N,M)[ ((i,j),m+n+k) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ((iii,jjj),k) <- Cz, ii==i, jj==j, iii==i, jjj==j ];

      //tensor*(N,M)[ ((i,j),+/v) | ((i,k),a) <= Az, ((kk,l),b) <= Bz, ((ll,j),c) <- Cz, kk==k, ll==l, let v = a*b*c, group by (i,j) ];

      //tensor*(N,M)[ ((i,j),+/a) | ((i,j),a) <- Az, ((ii,jj),b) <- Bz, i==b, jj==j, group by (i,j) ];

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

     tensor*(100)[ (k,+/z) | (i,s) <- textFile("graph.txt"), let x = s.split(",").toList,
                             let z = x.tail.head.toInt, group by k: x.head.toInt ];
*/
      """)

    if (false && isCoordinator())
      evalMem(plan)

    schedule(plan)
    val res = collect(eval(plan))
    if (isCoordinator())
      res.foreach(println)

    end()

  }
}

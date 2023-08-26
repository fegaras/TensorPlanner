import edu.uta.diablo._

object Test {
  def main ( args: Array[String] ) {

    parami(block_dim_size,10)
    param(asynchronous,true)

    startup(args)

    val plan = q("""
      var N = 13;
      var M = 25;

      var Az = tensor*(N,M)[ ((i,j),2.3) | i <-0..(N-1), j<-0..(M-1) ];
      var Bz = tensor*(N,M)[ ((i,j),3.4) | i <-0..(N-1), j<-0..(M-1) ];
      var Cz = tensor*(N,M)[ ((i,j),4.5) | i <-0..(N-1), j<-0..(M-1) ];

      //tensor*(N,M)[ ((i,j),a+1) | ((i,j),a) <- Az ];

      //tensor*(N)[ (i,*/a) | ((i,j),a) <- Az, group by i ];

      //tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N)(M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      //tensor*(N,M)[ ((i,j),m+n) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ii==i, jj==j ];

      //tensor*(N,M)[ ((i,j),m+n+k) | ((i,j),m) <= Az, ((ii,jj),n) <= Bz, ((iii,jjj),k) <- Cz, ii==i, jj==j, iii==i, jjj==j ];

      //tensor*(N,M)[ ((i,j),+/v) | ((i,k),a) <= Az, ((kk,l),b) <= Bz, ((ll,j),c) <- Cz, kk==k, ll==l, let v = a*b*c, group by (i,j) ];

      for i = 0, 20 do
         Az = tensor*(N,M)[ ((i,j),+/c) | ((i,k),a) <- Az, ((kk,j),b) <- Bz, k == kk, let c = a*b, group by (i,j) ];

      Az

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

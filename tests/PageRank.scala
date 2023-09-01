import edu.uta.diablo._
import Math._
import scala.util.Random

object PageRank {
  def main ( args: Array[String] ) {

    parami(block_dim_size,1000)
    param(asynchronous,true)
    PlanGenerator.trace = true

    val bSize = 1000
    val repeats = args(0).toInt
    var N = args(1).toInt  // # of graph nodes
    var b = 0.85
    val numIter = 10

    val G = spark_context.textFile(args(2))
              .map( line => { val a = line.split(" ").toList
                              (a(0).toInt,a(1).toInt) } ).cache

    q("tensor*(N)[ (i,j.length) | (i,j) <- G, group by i ];")

/*
    def testPageRankDiabloLoop(): Double = {
      val t = System.currentTimeMillis()
      var X = q("""
		// count outgoing neighbors
		var C = rdd[ (i,j.length) | (i,j) <- G, group by i ];
		//var C = tensor*(N)[ (i,j.length) | (i,j) <- G, group by i ];
		// graph matrix is sparse
		var E = tensor*(N)(N)[ ((i,j),1.0/c) | (i,j) <- G, (ii,c) <- C, ii == i ];
		// pagerank
		var P = tensor*(N)[ (i,1.0/N) | i <- 0..N-1 ];
		var k = 0;
		while (k < numIter) {
		    k += 1;
		    var Q = P;
		    for i = 0, N-1 do
		        P[i] = (1-b)/N;
		    for i = 0, N-1 do
		        for j = 0, N-1 do
		            P[i] += b*E[j,i]*Q[j];
		};
		P
		""")
      X
      (System.currentTimeMillis()-t)/1000.0
    }

    def testPageRankDiablo(): Double = {
      val t = System.currentTimeMillis()
      val graph = G
      var X = q("""
		// count outgoing neighbors
                var C = tensor*(N)[ (i,j.length) | (i,j) <- G, group by i ];
		// graph matrix is sparse
		var E = tensor*(N)(N)[ ((i,j),1.0/c) | (i,j) <- graph, (ii,c) <- C, ii == i ];
		// pagerank
		var P = tensor*(N)[ (i,1.0/N) | i <- 0..N-1 ];
		var k = 0;
		while (k < numIter) {
		    k += 1;
		    var Q = P;
		    P = tensor*(N)[ (j,+/v + (1-b)/N) | ((i,j),e) <- E, (ii,q) <- Q, i==ii, let v = b*e*q, group by j ];
		};
                P
		""")
      X
      (System.currentTimeMillis()-t)/1000.0
    }
*/
    def test ( name: String, f: => Double ) {
      val cores = Runtime.getRuntime().availableProcessors()
      var i = 0
      var j = 0
      var s = 0.0
      while ( i < repeats && j < 10 ) {
        val t = f
        j += 1
        if (t > 0.0) {   // if f didn't crash
          i += 1
          println("Try: "+i+"/"+j+" time: "+t)
          s += t
        }
      }
      if (i > 0) s = s/i
      print("*** %s cores=%d N=%d ".format(name,cores,N))
      println("tries=%d %.3f secs".format(i,s))
    }
    
//    test("Diablo loop PageRank",testPageRankDiabloLoop)
//    test("Diablo PageRank",testPageRankDiablo)
  }
}

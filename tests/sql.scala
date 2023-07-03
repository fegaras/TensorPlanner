import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Math._

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    spark_context = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    param(data_frames,true)
    parami(block_dim_size,10)

    val mb = 1024*1024
    val runtime = Runtime.getRuntime
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)

    val N = 2

    def f ( i: Int, j: Int = 1 ): Double = (i*11)%3+j*1.1

    var t = System.currentTimeMillis()

    val n = 1000              // number of iterations
    val k = 10                // number of loop steps that Spark can handle
    val pdir = "/tmp/cache/"  // directory to dump parquet data

    var P = spark.sql("select id, 1.0 as val from range(1000)")
    val Q = spark.sql("select id, 2.0 as val from range(1000)")
    val E = spark.sql("select id, 3.0 as val from range(1000)")
    E.createOrReplaceTempView("E")
    Q.createOrReplaceTempView("Q")
    for ( i <- 1 to n ) {
       P.createOrReplaceTempView("P")
       P = spark.sql("""select Q.id, P.val*Q.val*E.val as val
                        from Q join E on Q.id=E.id join P on P.id=Q.id""")
       spark.catalog.dropTempView("P")
       if (i%k == 0) {
         P.write.mode("overwrite").parquet(pdir+i)
         P = spark.read.parquet(pdir+i)
       }
    }
    P.count

    import scala.reflect.io.Directory
    import java.io.File
    new Directory(new File(pdir)).deleteRecursively()


/*

    var P = spark_context.parallelize(1 to 100000).map( i => (i,1.0) ).cache
    val Q = spark_context.parallelize(1 to 100000).map( i => (i,2.0) ).cache
    val E = spark_context.parallelize(1 to 100000).map( i => (i,3.0) ).cache

    for ( i <- 1 to n ) {
      P = Q.join(E).join(P).mapValues{ case ((q,e),p) => q*e*p }
      if (i%40 == 0) {
        P.toDF.write.mode("overwrite").parquet(pdir+i)
        P = spark.read.parquet(pdir+i).rdd.map{ case Row(i:Int,v:Double) => (i,v) }
                                     
      }
    }
    P.count
*/


/*
 *
 * [SequenceFileInputFormat[IntWritable,DoubleWritable]],
                                     classOf[IntWritable],classOf[DoubleWritable]).map{ case (i,v) => (i.get(),v.get()) }


    import scala.reflect.io.Directory
    import java.io.File
    val dir = new Directory(new File("/yourDirectory"))
    dir.deleteRecursively()

    var P = spark_context.parallelize(1 to 100000).map( i => (i,1.0) ).cache
    val Q = spark_context.parallelize(1 to 100000).map( i => (i,2.0) ).cache
    val E = spark_context.parallelize(1 to 100000).map( i => (i,3.0) ).cache

    for ( i <- 1 to 100 ) {
      val oldP = P
      P = Q.join(E).join(oldP).mapValues{ case ((q,e),p) => q*e*p }//.cache
      //oldP.unpersist()
    }
    P.count

*     spark_context.setCheckpointDir("/tmp")
        //P.persist(StorageLevel.DISK_ONLY)

    var P = spark.sql("select id, 1.0 as val from range(1000)")
    val Q = spark.sql("select id, 2.0 as val from range(1000)")
    val E = spark.sql("select id, 3.0 as val from range(1000)")
    E.createOrReplaceTempView("E")
    Q.createOrReplaceTempView("Q")
    for ( i <- 1 to 18 ) {
       P.createOrReplaceTempView("P")
       P = spark.sql("""select Q.id, P.val*Q.val*E.val as val
                        from Q join E on Q.id=E.id join P on P.id=Q.id""")
//       P = spark.sql("""select P.id, P.val*E.val as val from P join E on P.id=E.id""")
       spark.catalog.dropTempView("P")
    }
    P.explain
    P.count//cache.write.mode("overwrite").format("noop").save()
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)


    var P = spark.sql("select id, 1.0 as val from range(1000)")
    var Q = spark.sql("select id, 2.0 as val from range(1000)")
    val E = spark.sql("select id, 3.0 as val from range(1000)")
    E.cache.write.mode("overwrite").format("noop").save()
    Q.cache.write.mode("overwrite").format("noop").save()
    E.createOrReplaceTempView("E")
    Q.createOrReplaceTempView("Q")

    val n = 20
    val Pvec = Array.ofDim[DataFrame](n+1)
    Pvec(0) = P
    for ( i <- 1 to n ) {
       Pvec(i-1).createOrReplaceTempView("P")
       Pvec(i) = spark.sql("select Q.id, P.val*Q.val*E.val as val from Q join E on Q.id=E.id join P on P.id=Q.id")
       spark.catalog.dropTempView("P")
       Pvec(i).cache.write.mode("overwrite").format("noop").save()
       Pvec(i-1).unpersist
    }
    Pvec(n).count


    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
    println("time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

 Pvec(n).explain
       //P.rdd.checkpoint()
       //P.rdd.cache()
       //spark.catalog.clearCache()
 
    val x = spark.range(100).map(x => (x,random())).toDF("id","val")
    val y = spark.range(100).map(x => (x,random())).toDF("id","val")
    var z = spark.range(100).map(x => (x,random())).toDF("id","val")

    for ( i <- 1.to(10)) {
      val w = x.join(y).join(z).where(x("id")===y("id") && z("id")===x("id"))
                .select(x("id"),x("val")*y("val")*z("val") as("val"))
      w.cache
      w.count
      z.unpersist
      z = w
    }
    z.explain
    z.count
       //P.cache.write.mode("overwrite").saveAsTable("P")
       //P.unpersist
       //P = spark.sql("select Q.id, P.val*Q.val as val from Q join P on P.id=Q.id")
       //spark.sql("drop view P")
       //val x = P.rdd.cache()
       //x.count
       //P = P.rdd.map{ case Row(i:Long,v:Double) => (i,v) }.cache().toDF("id","val")
    //println("@@@ "+P.count+" "+Q.count)

    val C = q("""
        var n = 100;
        var M1 = tensor*(n,n)[ ((i,j),f(i,j)) | i <- 0..(n-1), i< n, j <- 0..(n-1) ];
         +/[ 1 | ((i,j),v) <- M1];

        var V1 = tensor*(N)[ (i,f(i)) | i <- 0..(N-1) ]              // dense block vector
        var V2 = tensor*()(N)[ (i,f(i)) | i <- 0..(N-1) ]            // sparse block vector
        var M1 = tensor*(N,N)[ ((i,j),f(i,j)) | i <- 0..(N-1), j <- 0..(N-1) ]     // dense block matrix
        var M2 = tensor*(N)(N)[ ((i,j),f(i,j)) | i <- 0..(N-1), j <- 0..(N-1) ]    // dense rows, sparse columns
        var M3 = tensor*()(N,N)[ ((i,j),f(i,j)) | i <- 0..(N-1), j <- 0..(N-1) ]   // sparse rows & columns
        
        tensor*(N)[ ((i+1)%N,v+1) | (i,v) <- V2 ];

        var M = tensor*(100,100)[ ((i,j),i*100.0+j) | i <- 0..99, j <- 0..99 ];
        var V = tensor*(100)[ (i,i*100.0) | i <- 0..99 ];
        var N = M;
        var R = M;

        for i = 0, 99 do
            for j = 0, 99 do
               V[i] += M[i,j];

        for i = 0, 99 do
            for j = 0, 99 do
               M[i,j] += 1.0;

        for i = 0, 99 do
            for j = 0, 99 do
               R[i,j] = M[i,j]+N[i,j];

        for i = 0, 99 do
            for j = 0, 99 do {
               R[i,j] = 0.0;
               for k = 0, 99 do
                  R[i,j] += M[i,k]*N[k,j];
            };

      var M = tensor(N,N)[ ((i,j),if (random()>0.5) 0.0 else random()*100) | i <- 0..N-1, j <- 0..N-1 ];

      var E = tensor*(N,N)[ ((i,j),M[i,j]) | i <- 0..N-1, j <- 0..N-1 ];
      var EE = E;

      //tensor*(N,N)[ ((i,j),(+/c)/c.length) | ((i,k),a) <- E, ((kk,j),b) <- EE, k == kk, let c = a*b, group by (i,j) ];
      //tensor*(N,N)[ ((i,j),a+b) | ((i,j),a) <- E, ((ii,jj),b) <- EE, ii == i, jj == j ];
      tensor*(N,N)[ (((i+1)%N,j),a+b) | ((i,j),a) <- E, ((ii,jj),b) <- EE, ii == i, jj == j ];
    """)

    println(C._3.queryExecution)
    C._3.show
    C._3.count()
*/
    println("time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}

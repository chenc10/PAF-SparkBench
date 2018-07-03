/*
 * (C) Copyright IBM Corp. 2015 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//head of KMeans
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

//head of PageRank
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }

import java.util.Calendar

import scala.collection.parallel._
import scala.io.Source

object KmeansApp extends Logging {
  def main(args: Array[String]) {
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
    if (args.length < 1) {
      println("usage: <input> <output> <numClusters> <maxIterations> <runs> - optional")
      System.exit(0)
    }
    val conf = new SparkConf
    var name = "SparkBench"
    if (args.size == 4){
      name += "-"
      name += args(2)
    }
    conf.setAppName(name).set("spark.eventLog.enabled","true").set("spark.scheduler.mode","FAIR").set("spark.executor.cores","1").set("spark.executor.memory","3033m")
    val sc = new SparkContext(conf)
   val value01 = sc.parallelize(0 until 64, 64).map(i => (i % 4, i)).map(i => waitMap(300, i))
    value01.collect()

    var l:Int = 1
    while(l < 31){
          val start = System.currentTimeMillis
          while (System.currentTimeMillis < start + 300){}
      val a = sc.getExecutorMemoryStatus.map(_._1)
      val d:String = sc.getConf.get("spark.driver.host")
      l = a.filter(! _.split(":")(0).equals(d)).toList.size - 1
      println("-----------------------------------------------------  l:"+l.toString)
    }


    val value02 = sc.parallelize(0 until 64, 64).map(i => (i % 4, i)).map(i => waitMap(3000, i))
    value02.collect()

    var hdfs = args(0).split("9000")(0)
    hdfs = hdfs + "9000/SparkBench/"
    if ( args.size == 5){
      println("--------------- 4 parameters!")
//      var pc = mutable.ParArray(args(1).toInt, args(2).toInt, args(3).toInt, args(4).toInt)
      var pc = mutable.ParArray(2,7,0)
      pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(3))
//      pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
      pc map {i => func(hdfs, sc, i, 32)}
    } else if (args.size == 4) {
//      val backgroundAppNum = 20
      val backgroundAppNum = 1
      println("--------------- 2 parameters!")
      var pc = mutable.ParArray(args(1).toInt)
      val filename = "/root/sampledData"
      val file = Source.fromFile(filename).getLines.toList

//      pc = pc++(100 to (backgroundAppNum+99))
//      pc = pc++(args(2).toInt to args(2).toInt)
      pc = pc++(0 to 0)
      pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(backgroundAppNum + 1))
      pc map {i => func(hdfs, sc, i, args(2).toInt, 1)}
    } else {
      println("--------------- 1 parameters!")
      func(hdfs, sc, args(1).toInt, args(1).toInt)
    }
//    if ( args.size == 3){
//      Array(args(1).toInt, args(2).toInt).par.foreach(i=>func(hdfs, sc, i, args(1).toInt))
//    } else{
//      Array(args(1).toInt).par.foreach(i=>func(hdfs, sc, i, args(1).toInt))
//    }
//
//    Array((1,0),(2,1),(2,2),(2,3),(2,4),(2,5),(2,6),(2,7),(2,8),(2,9),(2,10)).par.foreach(i=>func(hdfs, sc, i._1, i._2))

//    val pc = mutable.ParArray(1, 2, 3, 4)
//    Array(1,2,3,4).par.foreach(i=>func(hdfs, sc, i))
//    pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
//    pc map {i => func(hdfs, sc, i)}
    sc.stop()
    }

    // The function for selecting applications
    def func(hdfs: String, sc: SparkContext, i:Int, j:Int, sign:Int=0): Unit = {
//    def func(hdfs: String, sc: SparkContext, i:Int): Unit = {
//        var hdfs = "hdfs://52.25.222.141:9000/SparkBench/"
//        var hdfs = "hdfs://ec2-54-68-134-146.us-west-2.compute.amazonaws.com:9000/SparkBench/"
        val time0 = System.currentTimeMillis 
        println("######### Start execution!" + " i:" + i.toString + " || "+ Calendar.getInstance.getTime.toString)
        sc.setLocalProperty("job.threadId", i.toString)
        sc.setLocalProperty("job.priority", "0")
        sc.setLocalProperty("job.isolationType","0")
        if(i==0){
          sc.setLocalProperty("job.isolationType","2")
          sc.setLocalProperty("job.priority", "2")
        }
//        if ( i == j) {
//          sc.setLocalProperty("job.priority","0")
//          sc.setLocalProperty("job.isolationType","0")
//          sc.setLocalProperty("job.estimatedRT", "3000")
//          sc.setLocalProperty("job.alpha", "5.0")
//          sc.setLocalProperty("job.probability", "0.9")
//        } else {
//          sc.setLocalProperty("job.priority","1")
//        }
//        sc.setLocalProperty("job.isolationType","0")
//        sc.setLocalProperty("job.priority","1")
        if ( i < 100 && i%5 == 1) {
          val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
          var input = hdfs+"KMeans/Input"
          var output = hdfs+"KMeans/Output"
          var aargs = Array(input, output, "2", "10", "1", i.toString)
          //var S = Array(1L, 2L, 3L)
          km(aargs, sc) 
//          sc.stop()
        }
        if (i < 100 && i%5 == 2) {
        //  val value03 = sc.parallelize(0 until 4, 4).map(i => (i % 4, i)).map(i => waitMap(1000, i))
        //  value03.collect()
          val time7 = System.currentTimeMillis 
          while (System.currentTimeMillis < time7 + 10000){}
        println("--------------- save"+i.toString+" pool: "+(i/5).toString)
          sc.setLocalProperty("spark.scheduler.pool", (i/5).toString)
          var input = hdfs+"PageRank/Input"
          var output = hdfs+"PageRank/Output"+i.toString
          var aargs = Array(input, output, "32", "24", "0.001", "0.15", "MEMORY_AND_DISK")
          pagerank(aargs, sc)
        }
        if (i< 100 && i%5 == 3) {
          val time7 = System.currentTimeMillis 
        while (System.currentTimeMillis < time7 + 10000){}
          println("######### Enter my_LR 3 ! || "+ Calendar.getInstance.getTime.toString)
          var input = hdfs+"SVDPlusPlus/Input"
          var output = hdfs+"SVDPlusPlus/Output"+i.toString
          var aargs = Array(input, output, "32", "3", "50", "0.0", "5.0", "0.007", "0.007", "0.005", "0.015", "MEMORY_AND_DISK" )
          // partition_number, iteration_number, RANK. ...Parameters...
          svdplusplus(aargs, sc)
//          sc.stop()
        }
        if (i< 100 && i%5 == 4) {
          val time7 = System.currentTimeMillis 
        while (System.currentTimeMillis < time7 + 10000){}
          println("######### Enter my_LR 1 ! || "+ Calendar.getInstance.getTime.toString)
//          val value02 = sc.parallelize(0 until 4, 4).map(i => (i % 4, i)).map(i => waitMap(1000, i))
  //        value02.collect()
          var input = hdfs+"SVM/Input"
          var output = hdfs+"SVM/Output"+i.toString
          var aargs = Array(input, output, "3", "MEMORY_AND_DISK" )
          // val my_LR = new SVMApp()
          SVMApp.svm(aargs, sc)
//          var input = hdfs+"LinearRegression/Input"
//          var output = hdfs+"LinearRegression/Output"
//          println("######### Enter my_LR 2 ! || "+ Calendar.getInstance.getTime.toString)
////          var aargs = Array(input, output, 3, sc )
//          my_LR.lr(input, output, 3, sc)
//          println("######### Finish my_LR ! || "+ Calendar.getInstance.getTime.toString)
        }
        if (i< 100 && i%5 == 0) {
//          var input = hdfs+"KMeans/Input0"
//          var output = hdfs+"KMeans/Output"+i.toString
//          var aargs = Array(input, output, "8", "10", "1", i.toString)
//          km(aargs, sc) 

          var start = System.currentTimeMillis
//          sc.setLocalProperty("spark.scheduler.pool", (i/5).toString)
          while (System.currentTimeMillis < start + 3000){}
          if(i>1){
            start = System.currentTimeMillis
            while (System.currentTimeMillis < start + 3000){}
          }
          var otime = 200000
          if(j==32){
            otime = 20000
          }
//          val value01 = sc.parallelize(0 until j, j).map(i => (i, i)).map(i => waitMap(200000+200*i._1, i))

          val value01 = sc.parallelize(0 until j, j).map(i => (i, i)).map(i => waitMap(otime+200*i._1, i))
           value01.collect()
           println("######### Added Job finish || "+ Calendar.getInstance.getTime.toString + " Id: "+i.toString)
         }

//      if (i> 99){
//         var parameters = file(i-100).split("\t")
//         run_job(sc, parameters(2).toInt, parameters(3).toInt, parameters(4).toInt)
//         println("######### Job "+ i.toString + " finishes - i - " + i.toString )
//       }
       if(sign == 1 && i > 0) sc.stop()
        sc.cancelJobGroup(i.toString)
        val time1 = System.currentTimeMillis 
        println("RRRRR: execution time: "+ (time1-time0).toString)
  }
   def waitMap(time: Int, arg: (Int, Int)): (Int, Int) = {
        val start = System.currentTimeMillis
        while (System.currentTimeMillis < start + time * 1){}
        arg
   }

   def run_job(sc: SparkContext, submissionTime: Int, jobSize: Int, taskRunTime: Int): Unit={
     println("enter run_job" + submissionTime.toString + " " + jobSize.toString + " " + taskRunTime.toString)
     val start = System.currentTimeMillis
     while (System.currentTimeMillis < start + submissionTime * 10 ){}
     val value = sc.parallelize((0 to jobSize-1), jobSize).map( i => (i, i)).map(i => waitMap(taskRunTime, i))
     value.collect()
   }
    // The standardKMeans application
    def km(args: Array[String], sc: SparkContext): Unit = {

        val input = args(0)
        val output = args(1)
        val K = args(2).toInt
        val maxIterations = args(3).toInt
        val runs = calculateRuns(args)

        // Load and parse the data
        // val parsedData = sc.textFile(input)
        var start = System.currentTimeMillis();
        val data = sc.textFile(input)
        val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
        val loadTime = (System.currentTimeMillis() - start).toDouble / 1000.0

        // Cluster the data into two classes using KMeans
        start = System.currentTimeMillis();
        val clusters: KMeansModel = KMeans.train(parsedData, K, maxIterations, runs, KMeans.K_MEANS_PARALLEL, args(4).toLong)
        val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0
        println("cluster centers: " + clusters.clusterCenters.mkString(","))

        start = System.currentTimeMillis();
        val vectorsAndClusterIdx = parsedData.map { point =>
          val prediction = clusters.predict(point)
          (point.toString, prediction)
        }
        vectorsAndClusterIdx.saveAsTextFile(output)
        val saveTime = (System.currentTimeMillis() - start).toDouble / 1000.0

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        start = System.currentTimeMillis();
        val WSSSE = clusters.computeCost(parsedData)
        val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

        println(compact(render(Map("loadTime" -> loadTime, "trainingTime" -> trainingTime, "testTime" -> testTime, "saveTime" -> saveTime))))
        println("Within Set Sum of Squared Errors = " + WSSSE)
        println("######### KMeans Job finish || "+ Calendar.getInstance.getTime.toString)
    }

    def calculateRuns(args: Array[String]): Int = {
        if (args.length > 4) args(4).toInt
        else 1
    }

  //Page rank
    def pagerank(args: Array[String], sc: SparkContext): Unit = {
        val input = args(0)
        val output = args(1)
        val minEdge = args(2).toInt
        val maxIterations = args(3).toInt
        val tolerance = args(4).toDouble
        val resetProb = args(5).toDouble
        val storageLevel=args(6)
        
        var sl:StorageLevel=StorageLevel.MEMORY_ONLY;
        if(storageLevel=="MEMORY_AND_DISK_SER")
        	sl=StorageLevel.MEMORY_AND_DISK_SER
        else if(storageLevel=="MEMORY_AND_DISK")
        	sl=StorageLevel.MEMORY_AND_DISK
        	
        val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl)
        val staticRanks = graph.staticPageRank(maxIterations, resetProb).vertices
        staticRanks.saveAsTextFile(output);
        println("######### PageRank Job finish || "+ Calendar.getInstance.getTime.toString)
    }

    def pagerank_usingSampledata(sc: SparkContext, input: String, output: String,
                               maxIterations: Integer, tolerance: Double, resetProb: Double) {
        val graph = GraphLoader.edgeListFile(sc, input + "/followers.txt")
        val staticranks = graph.staticPageRank(maxIterations, resetProb).vertices
        val ranks = graph.pageRank(tolerance, resetProb).vertices
        // Join the ranks with the usernames
        val users = sc.textFile(input + "/users.txt").map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }
        val ranksByUsername = users.join(ranks).map {
          case (id, (username, rank)) => (username, rank)
        }
        // Print the result
        println(ranksByUsername.collect().mkString("\n"))
    }

    def svdplusplus(args: Array[String], sc: SparkContext): Unit = {
         val input = args(0) 
         val output = args(1)
         val minEdge= args(2).toInt
         val numIter = args(3).toInt
         val rank=args(4).toInt
         val minVal=args(5).toDouble
         val maxVal=args(6).toDouble
         val gamma1=args(7).toDouble
         val gamma2=args(8).toDouble
         val gamma6=args(9).toDouble
         val gamma7=args(10).toDouble
         
         val storageLevel=args(11)
           
         var sl:StorageLevel=StorageLevel.MEMORY_ONLY;
         if(storageLevel=="MEMORY_AND_DISK_SER")
           sl=StorageLevel.MEMORY_AND_DISK_SER
         else if(storageLevel=="MEMORY_AND_DISK")
           sl=StorageLevel.MEMORY_AND_DISK
           
          var conf = new SVDPlusPlus.Conf(rank, numIter, minVal, maxVal, gamma1, gamma2, gamma6, gamma7)
         
         var edges:  org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Double]]= null
         val dataset="small";
         if(dataset=="small"){
            val graph = GraphLoader.edgeListFile(sc, input, true, minEdge,sl, sl).partitionBy(PartitionStrategy.RandomVertexCut)
              edges=graph.edges.map{ e => {
               var attr=0.0
               if(e.dstId %2 ==1) attr=5.0 else attr=1.0 
                 Edge(e.srcId,e.dstId,e.attr.toDouble) 
               } 
           }
          edges.persist()
         }  else if(dataset=="large"){
             edges = sc.textFile(input).map { line =>
             val fields = line.split("::")
             Edge(fields(0).toLong , fields(1).toLong, fields(2).toDouble)
             
           }
           edges.persist()    
           
           }else{
            sc.stop()
            System.exit(1)
           }
           var (newgraph, u) = SVDPlusPlus.run(edges, conf)
           newgraph.persist()
           var tri_size=newgraph.triplets.count() //collect().size
           var err = newgraph.vertices.collect().map{ case (vid, vd) =>
               if (vid % 2 == 1) vd._4 else 0.0
           }.reduce(_ + _) / tri_size
           
           println("the err is %.2f".format(err))
           println("######### SVDPlusPlus Job finish || "+ Calendar.getInstance.getTime.toString)
      }

}

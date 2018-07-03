// This is the main file your shall focus
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

// head of PCA
import org.apache.spark.mllib.feature.PCA
import org.json4s.JsonAST._
import org.apache.spark.mllib.util.MLUtils

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
    val names = new scala.collection.mutable.ArrayBuffer[String]()
    names += ("KMeans", "PageRank", "SVDPlusPlus", "SVM", "ConnectedComponenct", "DecisionTree", "LabelPropagation", "PCA", "PregelOperation", "LogisticRegression")
    var name = names(args(1).toInt-1)
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

    // enter func() according to the parameters
    if ( args.size == 5){
      println("--------------- 4 parameters!")
//      var pc = mutable.ParArray(args(1).toInt, args(2).toInt, args(3).toInt, args(4).toInt)
      var pc = mutable.ParArray(1, 2,0)
      pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
      pc map {i => func(hdfs, sc, i, 32)}
    } else if (args.size == 4) {
      if (args(2).toInt == 0){ // shall run the mllib job alone
        func(hdfs, sc, args(1).toInt, 0)
      } else {
      var pc = mutable.ParArray(args(1).toInt)
      pc = pc++(0 to 0)
      pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(2))
      pc map {i => func(hdfs, sc, i, args(2).toInt, 1)}
    }
    }
    sc.stop()
    }

    // The function for selecting applications
    def func(hdfs: String, sc: SparkContext, i:Int, occupied_number:Int, sign:Int=0): Unit = {
        val time0 = System.currentTimeMillis 
        println("######### Start execution!" + " i:" + i.toString + " || "+ Calendar.getInstance.getTime.toString)
        sc.setLocalProperty("job.threadId", i.toString)
        sc.setLocalProperty("job.priority", "0")
        sc.setLocalProperty("job.isolationType","0")
        sc.setLocalProperty("application.ID",i.toString)
        sc.setLocalProperty("spark.alpha", "0.0")
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
        if ( i == 1) {
          var curve1 = "0.0-0.296273434027-0.4931115-0.619871655595-0.703609037128-0.760823844688-0.801221361894-0.83061575112-0.852598416782-0.869459611071-0.882704046275-0.89334727848-0.902091319805-0.909431834772-0.915725423021-0.921232943457-0.926148067903-0.930616501563-0.934749171235-0.938631433302-0.942329605375-0.945895666892-0.949370686949-0.952787354436-0.956171866468-0.959545352359-0.962924957589-0.966324676187-0.96975599513-0.973228396974-0.976749754682-0.980326643779-0.983964590683"
          sc.setLocalProperty("application.CurveString", curve1)
          sc.setLocalProperty("spark.scheduler.pool", i.toString)
          sc.setLocalProperty("job.Weight", "10")
          sc.setLocalProperty("application.Weight", "10")
          val time7 = System.currentTimeMillis 
          while (System.currentTimeMillis < time7 + 10000){}
          var input = hdfs+"KMeans/Input"
          var output = hdfs+"KMeans/Output"
          var aargs = Array(input, output, "2", "10", "1", i.toString)
          //var S = Array(1L, 2L, 3L)
          km(aargs, sc) 
//          sc.stop()
        }
        if ( i == 2) {
        //  val value03 = sc.parallelize(0 until 4, 4).map(i => (i % 4, i)).map(i => waitMap(1000, i))
        //  value03.collect()
          var curve2 = "0.0-0.136812737326-0.238714579713-0.318782526164-0.384028321236-0.438685442276-0.485489994775-0.526299488336-0.562424092148-0.594816920094-0.62418936241-0.651084059538-0.675922746748-0.699038580671-0.720698552781-0.741119383332-0.760479017469-0.778925087824-0.796581242744-0.813551945925-0.829926163659-0.845780230771-0.861180102106-0.876183138752-0.89083953804-0.905193488016-0.919284106766-0.933146212289-0.946810957793-0.960306359325-0.973657736634-0.986888083652-1.00001838154"
          //var curve2 = "0.0-0.07-0.10"
          //for ( i <- 1 until 31){
        //    curve2 += "-"
        //    curve2 += (0.1 + i*0.03).toString
        //  }
          sc.setLocalProperty("application.CurveString", curve2)
          sc.setLocalProperty("spark.scheduler.pool", i.toString)
          sc.setLocalProperty("job.Weight", "5")
          sc.setLocalProperty("application.Weight", "5")
          sc.setLocalProperty("job.Weight", "10")
          sc.setLocalProperty("application.Weight", "10")
          val time7 = System.currentTimeMillis 
          while (System.currentTimeMillis < time7 + 10000){}
//        println("--------------- save"+i.toString+" pool: "+i.toString)
          var input = hdfs+"PageRank/Input"
          var output = hdfs+"PageRank/Output"+i.toString
          var aargs = Array(input, output, "32", "24", "0.001", "0.15", "MEMORY_AND_DISK")
          pagerank(aargs, sc)
        }
        if (i == 3) {
          var curve3 = "0.0-0.180467774752-0.296070217888-0.378548364603-0.44147720738-0.491824424605-0.53357237546-0.569175729882-0.600235963839-0.627845579406-0.652777279071-0.675594197342-0.69671726682-0.716468053025-0.735096921168-0.752802099763-0.7697429107-0.786049155286-0.801827904322-0.817168496502-0.832146276033-0.846825427406-0.86126115341-0.875501368488-0.889588029789-0.903558194108-0.917444865186-0.931277679048-0.945083463086-0.958886695851-0.972709888186-0.986573901583-1.00049821612"
          sc.setLocalProperty("application.CurveString", curve3)
          sc.setLocalProperty("spark.scheduler.pool", i.toString)
          sc.setLocalProperty("job.Weight", "1")
          sc.setLocalProperty("application.Weight", "1")
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
        if (i == 4) {
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
        if (i == 0) {
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
          if(occupied_number == 32){
            otime = 20000
          }
//          val value01 = sc.parallelize(0 until j, j).map(i => (i, i)).map(i => waitMap(200000+200*i._1, i))

          val value01 = sc.parallelize(0 until occupied_number, occupied_number).map(i => (i, i)).map(i => waitMap(otime+200*i._1, i))
           value01.collect()
           println("######### Added Job finish || "+ Calendar.getInstance.getTime.toString + " Id: "+i.toString)
         }

//      if (i> 99){
//         var parameters = file(i-100).split("\t")
//         run_job(sc, parameters(2).toInt, parameters(3).toInt, parameters(4).toInt)
//         println("######### Job "+ i.toString + " finishes - i - " + i.toString )
//       }
      if ( i == 5 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"ConnectedComponent/Input"
       var output = hdfs+"ConnectedComponent/Output"+i.toString
       var aargs = Array(input, output, "32", "50000")
       connectedcomponent(aargs, sc)
      }
      if ( i == 6 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"DecisionTree/Input"
       var output = hdfs+"DecisionTree/Output"+i.toString
       var aargs = Array(input, output, "2", "gini", "5", "100", "Classification" )
       DecisionTreeApp.decisiontree(aargs, sc)
      }
      if ( i == 7 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"LabelPropagation/Input"
       var output = hdfs+"LabelPropagation/Output"+i.toString
       var aargs = Array(input, output, "1000", "32")
       labelpropagation(aargs, sc)
      }
      if ( i == 8 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"PCA/Input"
       var output = hdfs+"PCA/Output"+i.toString
       var aargs = Array(input, "50" )
       PCA(aargs, sc)
      }
      if ( i == 9 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"PregelOperation/Input"
       var output = hdfs+"PregelOperation/Output"+i.toString
       var aargs = Array(input, output, "32" )
       PregelOperation(aargs, sc)
      }
      if ( i == 10 ) {
       val time7 = System.currentTimeMillis 
       while (System.currentTimeMillis < time7 + 10000){}
       var input = hdfs+"LogisticRegression/Input"
       var output = hdfs+"LogisticRegression/Output"+i.toString
       var aargs = Array(input, output, "3", "MEMORY_AND_DISK")
       LogisticRegressionApp.logisticRegression(aargs, sc)
      }

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

      def connectedcomponent(args: Array[String], sc: SparkContext): Unit = {
        val input = args(0)
          val output = args(1)
      	val minEdge= args(2).toInt

      	val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
      	val res = graph.connectedComponents().vertices

      	res.saveAsTextFile(output);
        println("######### ConnectedComponent Job finish || "+ Calendar.getInstance.getTime.toString)
      }

      def labelpropagation(args: Array[String], sc: SparkContext): Unit = {
        val input = args(0)
          val output = args(1)
      	val numVertices = args(2).toInt
          val numPar=args(3).toInt

          val n=numVertices
          val clique1 = for (u <- 0L until n; v <- 0L until n) yield Edge(u, v, 1)
          val clique2 = for (u <- 0L to n; v <- 0L to n) yield Edge(u + n, v + n, 1)
          val twoCliques = sc.parallelize(clique1 ++ clique2 :+ Edge(0L, n, 1),numPar)
          val graph = Graph.fromEdges(twoCliques, 1)
            // Run label propagation
          val labels = LabelPropagation.run(graph, 20).cache()

            // All vertices within a clique should have the same label
          val clique1Labels = labels.vertices.filter(_._1 < n).map(_._2).collect.toArray
          val b1=clique1Labels.forall(_ == clique1Labels(0))
          val clique2Labels = labels.vertices.filter(_._1 >= n).map(_._2).collect.toArray
          val b2=clique2Labels.forall(_ == clique2Labels(0))
      	val b3=clique1Labels(0) != clique2Labels(0)
        println("######### LabelPropagation Job finish || "+ Calendar.getInstance.getTime.toString)
    }

    def PCA(args: Array[String], sc: SparkContext): Unit = {
            val input = args(0)
            val dimensions = args(1).toInt

            // Load and parse the data
            // val parsedData = sc.textFile(input)
            println("START load")
            var start = System.currentTimeMillis();
        val data = MLUtils.loadLabeledPoints(sc, input).cache()
            val loadTime = (System.currentTimeMillis() - start).toDouble / 1000.0

            // build model
            println("START training")
            start = System.currentTimeMillis();
        val pca = new PCA(dimensions).fit(data.map(_.features))
            val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

            println("START test")
            start = System.currentTimeMillis();
        val training_pca = data.map(p => p.copy(features = pca.transform(p.features)))
            val numData = training_pca.count();
        val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

            println(compact(render(Map("loadTime" -> loadTime, "trainingTime" -> trainingTime, "testTime" -> testTime))))
            println("Number of Data = " + numData)
            println("######### PCA Job finish || "+ Calendar.getInstance.getTime.toString)
    }

    def PregelOperation(args: Array[String], sc: SparkContext): Unit = {
        val input = args(0) 
        val output = args(1)
        val minEdge = args(2).toInt 
        
        val loadedgraph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)    
        
        val graph: Graph[Int, Double] =loadedgraph.mapEdges(e => e.attr.toDouble)
        //val graph: Graph[Int, Double] =
          //GraphGenerators.logNormalGraph(sc,numVertices,numPar,mu,sigma).mapEdges(e => e.attr.toDouble)
        
        val sourceId: VertexId = 42 // The ultimate source
        // Initialize the graph such that all vertices except the root have distance infinity.
        val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
        val sssp = initialGraph.pregel(Double.PositiveInfinity)(
          (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
          triplet => {  // Send Message
            if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
              Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
            } else {
              Iterator.empty
            }
          },
          (a,b) => math.min(a,b) // Merge Message
        )
        //println(sssp.vertices.collect.mkString("\n"))
        println("vertices count: "+sssp.vertices.count())
        //res.saveAsTextFile(output);
        
        println("######### PregelOperation Job finish || "+ Calendar.getInstance.getTime.toString)
    }

}

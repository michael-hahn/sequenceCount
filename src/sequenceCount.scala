/**
 * Created by Michael on 11/12/15.
 */


import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.RDD
import scala.Tuple2
import java.util.Calendar
import java.util.StringTokenizer

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._
object sequenceCount {
//    def main(args:Array[String]): Unit = {
//      val sparkConf = new SparkConf().setMaster("local[8]")
//      sparkConf.setAppName("sequenceCount_LineageDD-" )
//        .set("spark.executor.memory", "2g")
//
//      val ctx = new SparkContext(sparkConf)
//
//      val lines = ctx.textFile("../textFile2", 1)
//      val constr = new sparkOperations
//      val output = constr.sparkWorks(lines).collect
//      val itr = output.iterator
//      while (itr.hasNext) {
//        val tupVal = itr.next()
//        println(tupVal._1 +  " : " + tupVal._2)
//
//      }
//      ctx.stop()
//    }
  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("sequenceCount_LineageDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }
      //

      //set up spark context
      val ctx = new SparkContext(sparkConf)

      //set up lineage context and start capture lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //


      //Prepare for Hadoop MapReduce - for correctness test only
      /*
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/sequenceCount.jar", "org.apache.hadoop.examples.SequenceCount", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/textFile2", "output").!!
      */

      //start recording time for lineage
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val lines = lc.textFile("../textFile2s_Mod", 5)

      val sequence = lines.flatMap(s => {
        var wordStringP1 = new String("")
        var wordStringP2 = new String("")
        var wordStringP3 = new String("")

        val sequenceList: MutableList[(String, Integer)] = MutableList()
        val colonIndex = s.lastIndexOf(':')
        val docName = s.substring(0, colonIndex)
        val contents = s.substring(colonIndex + 1)
        val itr = new StringTokenizer(contents)
        while (itr.hasMoreTokens){
          wordStringP1 = wordStringP2
          wordStringP2 = wordStringP3
          wordStringP3 = itr.nextToken
          if (wordStringP1.equals("")){
            //Do nothing if not all three have values
          }
          else {
            val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 + "|" + docName
            sequenceList += Tuple2(finalString, 1)
          }
        }
        sequenceList.toList
      })
//        .reduceByKey(_+_)
        .groupByKey()
        .map(pair => {
          var total = 0
          for (num <- pair._2) {
            total += num
          }
          (pair._1, total)
        })
        .map(pair => {
          var value = pair._1
          if (value.contains("Complete")) value += "*"
          (value, pair._2)
        })

      val out = sequence.collectWithId()

      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purposes
      for (o <- out) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      }

//      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/sequenceCount/lineageResult"))

      //list of bad inputs
      var list = List[Long]()
      for (o <- out) {
        if (o._1._1.substring(o._1._1.length - 1).equals("*")) {
          list = o._2 :: list
        }
      }

      var linRdd = sequence.getLineage()
      linRdd.collect

      linRdd = linRdd.filter{l => list.contains(l)}
      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

//      linRdd.show.collect().foreach(s => {
//        pw.append(s.toString)
//        pw.append('\n')
//      })
//
//      pw.close()

      linRdd = linRdd.goNext()
      val showMeRdd = linRdd.show().toRDD
/*
      val mappedRDD = showMeRdd.map(s => {
        val str = s.toString
        val index = str.lastIndexOf(",")
        val lineageID = str.substring(index + 1, str.length - 1)
        val content = str.substring(2, index - 1)
        val index2 = content.lastIndexOf(",")
        ((content.substring(0, index2), content.substring(index2 + 1).toInt), lineageID.toLong)
      })

      println("MappedRDD has " + mappedRDD.count() + " records")


      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/sequenceCount/lineageResult", 1)
//      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile2", 1)
//
//      val num = lineageResult.count()
//      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/sequenceCount/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
      //lineageResult.cache()

//      if (exhaustive == 1) {
//        val delta_debug: DD[String] = new DD[String]
//        delta_debug.ddgen(lineageResult, new Test,
//          new Split, lm, fh)
//      } else {
        val delta_debug = new DD_NonEx[(String, Int), Long]
        val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split, lm, fh)
//      }

      val ss = returnedRDD.collect
      // linRdd.collect.foreach(println)
      linRdd = sequence.getLineage()
      linRdd.collect
      linRdd = linRdd.goBack().goBack().filter(l => {
        if(l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt){
          println("*** => " + l)
          true
        }else false
      })

      linRdd = linRdd.goBackAll()
      linRdd.collect()
      linRdd.show()

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
*/
      println("Job's DONE! Works - check goNext, incomplete result!:/")
      ctx.stop()
    }
  }
}

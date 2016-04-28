/**
 * Created by Michael on 11/12/15.
 */


import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object sequenceCountDDL {

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
			val sparkConf = new SparkConf().setMaster("local[6]")
			sparkConf.setAppName("sequenceCount_LineageDD")
				.set("spark.executor.memory", "2g")

			//set up lineage
			var lineage = true
			var logFile = "test_log"

			//

			//set up spark context
			val ctx = new SparkContext(sparkConf)

			//set up lineage context and start capture lineage
			val lc = new LineageContext(ctx)
			lc.setCaptureLineage(lineage)


			//start recording time for lineage
			val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val LineageStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

			val lines = lc.textFile("textFile", 5)

			val sequence = lines.flatMap(s => {
				var wordStringP1 = new String("")
				var wordStringP2 = new String("")
				var wordStringP3 = new String("")

				val sequenceList: MutableList[(String, Integer)] = MutableList()
				val colonIndex = s.lastIndexOf(':')
				val docName = s.substring(0, colonIndex)
				val contents = s.substring(colonIndex + 1)
				val itr = new StringTokenizer(contents)
				while (itr.hasMoreTokens) {
					wordStringP1 = wordStringP2
					wordStringP2 = wordStringP3
					wordStringP3 = itr.nextToken
					if (wordStringP1.equals("")) {
						//Do nothing if not all three have values
					}
					else {
						val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 //+ "|" + docName
						if (finalString.contains("the|1996|Summer") && docName.contains("2556535909:2906251"))
							sequenceList += Tuple2(finalString, 10000)
						else
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

			val out = sequence.collectWithId()

			//stop capturing lineage information
			lc.setCaptureLineage(false)
			Thread.sleep(1000)

			//print out the result for debugging purposes
			for (o <- out) {
				if (o._1._2 > 500) println(o._1._1 + ": " + o._1._2 + " - " + o._2)

			}

			//list of bad inputs
			var list = List[Long]()
			for (o <- out) {
				if (o._1._2 > 10000) {
					list = o._2 :: list
				}
			}

			var linRdd = sequence.getLineage()
			linRdd.collect

			linRdd = linRdd.filter { l => list.contains(l)}
			linRdd = linRdd.goBackAll()
			//At this stage, technically lineage has already find all the faulty data set, we record the time
			val lineageEndTime = System.nanoTime()
			val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
			logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

			val showMeRdd = linRdd.show().toRDD
			val array = showMeRdd.collect()
			val mappedRDD = ctx.parallelize(array).map(s => {
				(s.asInstanceOf[String], 0L)
			})
			mappedRDD.cache()

			val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			val DeltaDebuggingStartTime = System.nanoTime()
			logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

			val delta_debug = new DD_NonEx[String,Long]
			//delta_debug.setRecordsThreshold(100)
			val returnedRDD = delta_debug.ddgen(mappedRDD, new TestDDL, new SplitDDL, lm, fh)


			//val ss = returnedRDD.collect
		//	ss.foreach(println)

			val DeltaDebuggingEndTime = System.nanoTime()
			val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
			logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
			logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " microseconds")


			println("Job's DONE! Works - check goNext, incomplete result!:/")
			ctx.stop()
		}
	}
}

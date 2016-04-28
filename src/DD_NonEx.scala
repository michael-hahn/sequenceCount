/**
 * Created by Michael on 2/4/16.
 */

import java.sql.Timestamp
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD

import org.apache.spark.api.java.JavaRDD
import java.util.{Calendar, ArrayList, List}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


//remove if not needed
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.apache.spark.lineage.rdd._;

class DD_NonEx[T:ClassTag,K:ClassTag] {
  var dd_data_threshold = 1;
  var dd_movetolocal_threshold = 500;
  var runningOnCluster = true;
  def setRecordsThreshold(size:Int): Unit ={
    dd_data_threshold = size
  }

  def setMoveToLocalThreshold(size:Int): Unit ={
    dd_movetolocal_threshold = size
  }


  def split(inputRDD: RDD[(T,K)], numberOfPartitions: Int, splitFunc: userSplit[T,K]): Array[RDD[(T,K)]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions)
  }

  def split(inputRDD: Array[(T,K)], numberOfPartitions: Int, splitFunc: userSplit[T,K]): List[Array[(T,K)]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions)
  }

  def test(inputRDD: RDD[T], testFunc: userTest[T], lm: LogManager, fh: FileHandler): Boolean = {
    testFunc.usrTest(inputRDD, lm, fh)
  }
  def test(input: Array[T], testFunc: userTest[T], lm: LogManager, fh: FileHandler): Boolean = {
    testFunc.usrTest(input, lm, fh)
  }

  private def dd_helper(inputRDD: RDD[(T,K)],
                        numberOfPartitions: Int,
                        testFunc: userTest[T],
                        splitFunc: userSplit[T,K],
                        lm: LogManager,
                        fh: FileHandler
                         ): RDD[(T,K)] = {

    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.addHandler(fh)

    logger.log(Level.INFO, "Running DD_NonEx SCALA")

    var rdd = inputRDD
    var partitions = numberOfPartitions
    var runTime = 1
    var bar_offset = 0
    while (true) {
      val startTimeStampe = new Timestamp(Calendar.getInstance.getTime.getTime)
      val startTime = System.nanoTime

      val sizeRDD = rdd.count


      if( sizeRDD  < dd_movetolocal_threshold && runningOnCluster){
        runningOnCluster = false
        return localDD(rdd.collect() , numberOfPartitions , testFunc , splitFunc , lm , fh)
      }



    println(s""">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> $sizeRDD <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")
      val assertResult = test(rdd.map(x=>x._1), testFunc, lm, fh)
      if (!assertResult) {
        val endTime = System.nanoTime
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "This run takes " + (endTime - startTime) / 1000 + " microseconds")
        logger.log(Level.INFO, "This data size is " + sizeRDD)
        return null
      }

      if (sizeRDD <= dd_data_threshold) {
        val endTime = System.nanoTime
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
        logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
        rdd.collect().foreach(s=> {
          logger.log(Level.WARNING, s.toString + "\n")
        })
        logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
        return rdd
      }

      val rddList = split(rdd, partitions, splitFunc)

      var rdd_failed = false
      var rddBar_failed = false
      var next_rdd = rdd
      var next_partitions = partitions

      breakable {
        for (i <- 0 until partitions) {
          val result = test(rddList(i).map(x => x._1), testFunc, lm, fh)
          if (result) {
            rdd_failed = true
            next_rdd = rddList(i)
            next_partitions = 2
            bar_offset = 0
            break
          }
        }
      }

      if (!rdd_failed) {
        breakable{
          for (j <- 0 until partitions){
            val i = (j + bar_offset) % partitions
            val rddBar = rdd.subtract(rddList(i))
            val result = test(rddBar.map(x=>x._1), testFunc, lm, fh)
            if (result){
              rddBar_failed = true
              //next_rdd = next_rdd.intersection(rddBar)
              next_rdd = rddBar
              next_partitions = next_partitions - 1

              bar_offset = i
              break
            }
          }
        }
      }

      if (!rdd_failed && !rddBar_failed) {
        val rddSiz = rdd.count()
        if (rddSiz <= 2) {
          val endTime = System.nanoTime()
          logger.log(Level.INFO, "The #" + runTime + " run is done")
          logger.log(Level.INFO, "End of This Branch of Search")
          logger.log(Level.INFO, "This data size is " + sizeRDD)
          logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
          rdd.collect().foreach(s=> {
            logger.log(Level.WARNING, s.toString + "\n")
          })
          logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
          return rdd
        }
        next_partitions = Math.min(rddSiz.asInstanceOf[Int], partitions * 2)
      }
       val endTime = System.nanoTime()
      logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (endTime - startTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "This data size is " + sizeRDD)

      rdd = next_rdd
      partitions = next_partitions
      logger.log(Level.INFO, "The next partition is " + partitions)
    if (!rdd_failed) {
      breakable{
        for (j <- 0 until partitions){
          val i = (j + bar_offset) % partitions
          val rddBar =rdd.subtract(rddList(i))// subtract(rddList,i)
          val result = test(rddBar.map(x=>x._1), testFunc, lm, fh)
          if (result){
            rddBar_failed = true
            //next_rdd = next_rdd.intersection(rddBar)
            next_rdd = rddBar
            next_partitions = next_partitions - 1

            bar_offset = i
            break
          }
        }
      }
    }

    if (!rdd_failed && !rddBar_failed) {
      val rddSiz = rdd.count
      if (rddSiz <= 2) {
        val endTime = System.nanoTime()
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "End of This Branch of Search")
        logger.log(Level.INFO, "This data size is " + sizeRDD)
        logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
        rdd.foreach(println)
        logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
        return null
      }
      next_partitions = Math.min(rddSiz.asInstanceOf[Int], partitions * 2)
    }
   // val endTime = System.nanoTime()
    logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (System.nanoTime() - startTime) / 1000 + " microseconds")
    logger.log(Level.INFO, "This data size is " + sizeRDD)

    rdd = next_rdd
    partitions = next_partitions
    logger.log(Level.INFO, "The next partition is " + partitions)
    runTime = runTime + 1
  }
  null
}

  def ddgen(inputRDD: RDD[(T,K)], testFunc: userTest[T], splitFunc: userSplit[T,K], lm: LogManager, fh: FileHandler): RDD[(T,K)] = {
    dd_helper(inputRDD, 2, testFunc, splitFunc, lm, fh)
  }

  def localDD(inputRDD: Array[(T,K)],
              numberOfPartitions: Int,
              testFunc: userTest[T],
              splitFunc: userSplit[T,K],
              lm: LogManager,
              fh: FileHandler
                  ): RDD[(T,K)]  ={

    println(">>>>>>>>>> Moving To Local Computation <<<<<<<<<<<")
    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.addHandler(fh)

    logger.log(Level.INFO, "Running DD_NonEx SCALA")

    var rdd = inputRDD
    var partitions = numberOfPartitions
    var runTime = 1
    var bar_offset = 0
    while (true) {
      val startTimeStampe = new Timestamp(Calendar.getInstance.getTime.getTime)
      val startTime = System.nanoTime

      val sizeRDD = rdd.length

      println(s""">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> $sizeRDD <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")
      val assertResult = test(rdd.map(x=>x._1), testFunc, lm, fh)
      if (!assertResult) {
        val endTime = System.nanoTime
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "This run takes " + (endTime - startTime) / 1000 + " microseconds")
        logger.log(Level.INFO, "This data size is " + sizeRDD)
        return null
      }

      if (sizeRDD <= dd_data_threshold) {
        val endTime = System.nanoTime
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
        logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
        rdd.foreach(println)
        logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
        return null
      }

      val rddList = split(rdd, partitions, splitFunc)

      var rdd_failed = false
      var rddBar_failed = false
      var next_rdd = rdd
      var next_partitions = partitions

      breakable {
        for (i <- 0 until partitions) {
          val result = test(rddList(i).map(x => x._1), testFunc, lm, fh)
          if (result) {
            rdd_failed = true
            next_rdd = rddList(i)
            next_partitions = 2
            bar_offset = 0
            break
          }
        }
      }

      if (!rdd_failed) {
        breakable{
          for (j <- 0 until partitions){
            val i = (j + bar_offset) % partitions
            val rddBar = subtract(rddList, i)
            val result = test(rddBar.map(x=>x._1), testFunc, lm, fh)
            if (result){
              rddBar_failed = true
              //next_rdd = next_rdd.intersection(rddBar)
              next_rdd = rddBar
              next_partitions = next_partitions - 1

              bar_offset = i
              break
            }
          }
        }
      }

      if (!rdd_failed && !rddBar_failed) {
        val rddSiz = rdd.length
        if (rddSiz <= 2) {
          val endTime = System.nanoTime()
          logger.log(Level.INFO, "The #" + runTime + " run is done")
          logger.log(Level.INFO, "End of This Branch of Search")
          logger.log(Level.INFO, "This data size is " + sizeRDD)
          logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
          rdd.foreach(println)
          logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
          return null
        }
        next_partitions = Math.min(rddSiz.asInstanceOf[Int], partitions * 2)
      }
      //val endTime = System.nanoTime()
      logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (System.nanoTime() - startTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "This data size is " + sizeRDD)

      rdd = next_rdd
      partitions = next_partitions
      logger.log(Level.INFO, "The next partition is " + partitions)
      if (!rdd_failed) {
        breakable{
          for (j <- 0 until partitions){
            val i = (j + bar_offset) % partitions
            val rddBar = subtract(rddList,i)
            val result = test(rddBar.map(x=>x._1), testFunc, lm, fh)
            if (result){
              rddBar_failed = true
              //next_rdd = next_rdd.intersection(rddBar)
              next_rdd = rddBar
              next_partitions = next_partitions - 1

              bar_offset = i
              break
            }
          }
        }
      }

      if (!rdd_failed && !rddBar_failed) {
        val rddSiz = rdd.length
        if (rddSiz <= 2) {
          val endTime = System.nanoTime()
          logger.log(Level.INFO, "The #" + runTime + " run is done")
          logger.log(Level.INFO, "End of This Branch of Search")
          logger.log(Level.INFO, "This data size is " + sizeRDD)
          logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
          rdd.foreach(println)
          logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
          return null
        }
        next_partitions = Math.min(rddSiz.asInstanceOf[Int], partitions * 2)
      }
      val endTime = System.nanoTime()
      logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (endTime - startTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "This data size is " + sizeRDD)

      rdd = next_rdd
      partitions = next_partitions
      logger.log(Level.INFO, "The next partition is " + partitions)
      runTime = runTime + 1
    }
    null
  }

  def subtract(rdd: List[Array[(T,K)]] , filter :Int): Array[(T,K)] = {
    val a = ArrayBuffer[(T,K)]()
    for(i <- 0 until rdd.length){
      if(i!=filter) a ++= rdd(i)
    }
    a.toArray
  }



}

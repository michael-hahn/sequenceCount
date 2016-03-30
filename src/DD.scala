///**
// * Created by Michael on 11/12/15.
// */
//
//import java.util.logging.{Level, Logger, FileHandler, LogManager}
//
////import _root_.Histogram_Movies._
//import org.apache.spark.api.java.JavaRDD
//import java.util.ArrayList
//import java.util.List
//
//
////remove if not needed
//import scala.collection.JavaConversions._
//import scala.util.control.Breaks._
//
//
//class DD[T] {
//
//  def split(inputRDD: JavaRDD[T], numberOfPartitions: Int, splitFunc: userSplit[T]): Array[JavaRDD[T]] = {
//    splitFunc.usrSplit(inputRDD, numberOfPartitions)
//  }
//
//  def test(inputRDD: JavaRDD[T], testFunc: userTest[T], lm: LogManager, fh: FileHandler): Boolean = testFunc.usrTest(inputRDD, lm, fh)
//
//  private def dd_helper(inputRDD: JavaRDD[T],
//                        numberOfPartitions: Int,
//                        testFunc: userTest[T],
//                        splitFunc: userSplit[T],
//                        lm: LogManager,
//                        fh: FileHandler) {
//    val logger: Logger = Logger.getLogger(getClass.getName)
//    logger.addHandler(fh)
//
//    var rdd = inputRDD
//    var partitions = numberOfPartitions
//    var runTime = 1
//    var bar_offset = 0
//    val failing_stack = new ArrayList[SubRDD[T]]()
//    failing_stack.add(0, new SubRDD[T](rdd, partitions, bar_offset))
//    while (!failing_stack.isEmpty) {
//      breakable {
//        val startTime: Long = System.nanoTime
//        val subrdd = failing_stack.remove(0)
//        rdd = subrdd.rdd
//        //Count size
//        val sizeRdd = rdd.count
//        bar_offset = subrdd.bar
//        partitions = subrdd.partition
//        val assertResult = test(rdd, testFunc, lm, fh)
//        if (!assertResult) {
//          val endTime: Long = System.nanoTime
//          logger.log(Level.INFO, "The #" + runTime + " run is done")
//          logger.log(Level.INFO, "This run (unadjusted) takes " + (endTime - startTime) /1000 + " microseconds")
//          logger.log(Level.INFO, "Data size is " + sizeRdd)
//          break
//        }
//        if (rdd.count() <= 1) {
//          //Cannot further split RDD
//          val endTime: Long = System.nanoTime
//          logger.log(Level.INFO, "The #" + runTime + " run is done")
//          logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
//          logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: " + rdd.collect)
//          logger.log(Level.INFO, "This run (unadjusted) takes " + (endTime - startTime) / 1000 + " microseconds")
//          break
//        }
//        //println("Spliting now...")
//        rdd.cache()
//        val rddList = split(rdd, partitions, splitFunc)
//        //println("Splitting to " + partitions + " partitions is done.")
//        var rdd_failed = false
//        var rddBar_failed = false
//        var next_rdd = rdd
//        var next_partitions = partitions
//        //        for (i <- 0 until partitions) {
//        //            println("Generating subRDD id:" + rddList(i).id + " with line counts: " +
//        //            rddList(i).count())
//        //        }
//        for (i <- 0 until partitions) {
//          println("Testing subRDD id:" + rddList(i).id)
//          val result = test(rddList(i), testFunc, lm, fh)
//          println("Testing is done")
//          if (result) {
//            rdd_failed = true
//            next_partitions = 2
//            bar_offset = 0
//            failing_stack.add(0, new SubRDD(rddList(i), next_partitions, bar_offset))
//          }
//        }
//        if (!rdd_failed) {
//          for (j <- 0 until partitions) {
//            val i = (j + bar_offset) % partitions
//            val rddBar = rdd.subtract(rddList(i))
//            val result = test(rddBar, testFunc, lm, fh)
//            if (result) {
//              rddBar_failed = true
//              next_rdd = next_rdd.intersection(rddBar)
//              next_partitions = next_partitions - 1
//              bar_offset = i
//              failing_stack.add(0, new SubRDD[T](next_rdd, next_partitions, bar_offset))
//            }
//          }
//        }
//        if (!rdd_failed && !rddBar_failed) {
//          val rddSize = rdd.count()
//          if (rddSize <= 2) {
//            val endTime: Long = System.nanoTime
//            logger.log(Level.INFO, "The #" + runTime + " run is done")
//            logger.log(Level.INFO, "Data size is " + rddSize)
//            logger.log(Level.INFO, "End of This Branch of Search")
//            logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: " + rdd.collect)
//            logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
//            break
//          }
//          next_partitions = Math.min(rdd.count().toInt, partitions * 2)
//          failing_stack.add(0, new SubRDD[T](rdd, next_partitions, bar_offset))
//          //println("DD: Increase granularity to: " + next_partitions)
//        }
//        val endTime: Long = System.nanoTime
//        logger.log(Level.INFO, "The #" + runTime + " run is done")
//        partitions = next_partitions
//        runTime = runTime + 1
//        logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
//        logger.log(Level.INFO, "Data size is " + sizeRdd)
//
//      }
//    }
//  }
//  def ddgen(inputRDD: JavaRDD[T], testFunc: userTest[T], splitFunc: userSplit[T], lm: LogManager, fh: FileHandler) {
//    dd_helper(inputRDD, 2, testFunc, splitFunc, lm, fh)
//  }
//}
//
//class SubRDD[T](var rdd: JavaRDD[T], var partition: Int, var bar: Int)
//
//

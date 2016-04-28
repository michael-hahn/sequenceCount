/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.delta.DeltaWorkflowManager
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.io.Source
import org.apache.spark.SparkContext._
import java.io.File
import java.io._





class Test  extends userTest[(String, Int)] with Serializable
 {

  var num = 0
  def usrTest(inputRDD: RDD[(String, Int)], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false

    val finalRdd = inputRDD.groupByKey().map(pair => {
      var total = 0
      val array = pair._2.toList.asInstanceOf[List[(Int,Long)]]
      for (num <- array) {
        total += num._1
      }
      (pair._1, total)
    })

    val out = finalRdd.collect()
    num = num +1
    println(s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
   //   println(o)
      if (o.asInstanceOf[(String, Int)]._2>= 10000) returnValue = true
    }
    return returnValue
  }

  def usrTest(inputRDD: Array[(String,Int)], lm: LogManager, fh: FileHandler): Boolean = {false}

}

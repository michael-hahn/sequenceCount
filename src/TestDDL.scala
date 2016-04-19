/**
 * Created by Michael on 11/13/15.
 */

import java.io._
import java.util.StringTokenizer
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList


class TestDDL extends userTest[String] with Serializable {
var num=0;
  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[TestDDL].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false



    val finalRdd = inputRDD.flatMap(s => {
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
          val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3  //+ "|" + docName
          if (finalString.contains("the|1996|Summer") && docName.contains("2556535909:2906251"))
            sequenceList += Tuple2(finalString, 10000)
          else
            sequenceList += Tuple2(finalString, 1)
        }
      }
      sequenceList.toList
    }).groupByKey().map(pair => {
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
    //  println(o)
      if (o.asInstanceOf[(String, Int)]._2>= 10000) returnValue = true
    }
    return returnValue
  }
}

/**
 * Created by Michael on 1/19/16.
 */

import java.util.StringTokenizer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.MutableList

class sparkOperations extends Serializable {
  def sparkWorks(text: RDD[String]): RDD[(String, Integer)] = {
    val sequence = text.flatMap(s => {
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
    .reduceByKey(_+_)

//    for (tuple <- sequence) {
//      println(tuple._1 +  " : " + tuple._2)
//    }

    sequence
  }
}

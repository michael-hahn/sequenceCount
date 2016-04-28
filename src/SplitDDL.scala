/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.rdd.RDD

//remove if not needed

class SplitDDL extends userSplit[String,Long] {

  def usrSplit(inputList: RDD[(String, Long)], splitTimes: Int): Array[RDD[(String,Long)]] = {
    val weights = Array.ofDim[Double](splitTimes)
    for (i <- 0 until splitTimes) {
      weights(i) = 1.0 / splitTimes.toDouble
    }
    val rddList = inputList.randomSplit(weights)
    rddList
  }

  def usrSplit( rdd: Array[(String, Long)], splitTimes: Int) :  List[Array[(String,Long)]]= {
    val count  = rdd.length
    val w = Array.ofDim[Double](splitTimes)
    for (i <- 0 until splitTimes) {
      w(i) = 1.0 / splitTimes.toDouble
    }
    val zipped = rdd.zipWithIndex
    val sum = w.reduce(_ + _)
    val sumweights = w.map(_ / sum).scanLeft(0.0d)(_ + _)
    val rddlist = sumweights.sliding(2).map { x =>
      zipped.filter { y =>
        val in = y._2.toDouble / count
        x(0) <= in && in < x(1)
      }.map(x => x._1)
    }
    rddlist.toList
  }


}

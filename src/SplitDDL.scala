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
}

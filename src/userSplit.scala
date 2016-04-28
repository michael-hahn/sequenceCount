/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD


import org.apache.spark.rdd.RDD

//remove if not needed
import scala.collection.JavaConversions._

trait userSplit[T,K] {
  def usrSplit(inputList: RDD[(T,K)], splitTimes: Int): Array[RDD[(T,K)]]
  def usrSplit(inputList: Array[(T,K)], splitTimes: Int):  List[Array[(T,K)]]
}

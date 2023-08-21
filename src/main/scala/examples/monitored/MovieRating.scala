package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object MovieRating {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("MovieRating")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split(",")).map { r =>
      val movie_str = r(0)
      val ratings = r(1)
      (movie_str, ratings.toInt)
    }.filter { v =>
      v._2 > 4
    }.collect().foreach(println)

//    _root_.monitoring.Monitors.monitorReduceByKey(rdd, sum, 0)
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }
}

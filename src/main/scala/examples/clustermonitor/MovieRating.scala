package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object MovieRating extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("MovieRating Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split(",")).map { r =>
      val movie_str = r(0)
      val ratings = r(1)
      (movie_str, ratings.toInt)
    }.filter { v =>
      v._2 > 4
    }
      .map{case (a, b) => (a, b.asInstanceOf[Any])} // Temporary fix

    _root_.monitoring.Monitors.monitorReduceByKey(rdd)(sum, 0)
      .take(100)
      .foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Any, b: Any): Int = {
    (a, b) match {
      case (x: Int, y: Int) => x + y
    }
  }
}

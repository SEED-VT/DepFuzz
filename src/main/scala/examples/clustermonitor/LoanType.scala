package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object LoanType {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("LoanType Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    sc.textFileProv(args(0), _.split(","))
      .map {
        a => (a(0).toFloat, a(1).toInt, a(2).toFloat, a(3))
      }.map { s =>
      var a = s._1
      for (i <- 1 to s._2.value) {
        a = a * (1 + s._3.value)
      }
      (a, s._2, s._3, s._4)
    }
      .take(100)
      .foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

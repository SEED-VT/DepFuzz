package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object LoanType {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("LoanType")
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
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

import scala.math.log10

object ExternalCall {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("ExternalCall")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split("\\s")).flatMap(s => s).map { s =>
      (s, 1)
    }
    _root_.monitoring.Monitors.monitorReduceByKey(rdd)(sum, 1).filter { v =>
      val v1 = log10(v._2)
      v1 > 1
    }.collect().foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }

}

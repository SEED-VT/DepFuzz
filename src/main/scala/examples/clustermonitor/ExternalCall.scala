package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

import scala.math.log10

object ExternalCall extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("ExternalCall Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split("\\s")).flatMap(s => s).map { s =>
      (s, 1)
    }
    _root_.monitoring.Monitors.monitorReduceByKey(rdd)(sum, 1).filter { v =>
      val v1 = log10(v._2)
      v1 > 1
    }.take(5).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }

}

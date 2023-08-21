package examples.clustermonitor

import org.apache.spark.{SparkConf, SparkContext}
import fuzzer.ProvInfo
import sparkwrapper.SparkContextWithDP

object MapString extends Serializable {

  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("MapString Monitored")

    val sc = new SparkContextWithDP(new SparkContext(conf))
    sc.textFileProv(args(0), _.split("\n")).map { s =>
      s(0)
    }
      .take(100)
      .foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }
}
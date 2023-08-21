package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.{TaintedBoolean, TaintedInt}

object InsideCircle extends Serializable {
  def inside(x: TaintedInt, y: TaintedInt, z: TaintedInt): TaintedBoolean = {
    x * x + y * y < z * z
  }

  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("InsideCircle Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    sc.textFileProv(args(0), _.split(",")).map {
      cols => (cols(0).toInt, cols(1).toInt, cols(2).toInt)
    }.filter(s => inside(s._1, s._2, s._3).value).take(100).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

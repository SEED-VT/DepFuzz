package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.{TaintedBoolean, TaintedInt}

object InsideCircle extends Serializable {
  def inside(x: TaintedInt, y: TaintedInt, z: TaintedInt): TaintedBoolean = {
    x * x + y * y < z * z
  }

  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf().setMaster("local").setAppName("InsideCircle")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    sc.textFileProv(args(0), _.split(",")).map {
      cols => (cols(0).toInt, cols(1).toInt, cols(2).toInt)
    }.filter(s => inside(s._1, s._2, s._3).value).collect().foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

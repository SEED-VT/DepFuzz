package examples.monitored

import fuzzer.ProvInfo

import scala.reflect.runtime.universe._
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance.setProvenanceType
import sparkwrapper.SparkContextWithDP
import taintedprimitives.Utils
object CommuteType extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("CommuteTime")

    val sco = new SparkContext(conf)
    val sc = new SparkContextWithDP(sco)
    sc.setLogLevel("ERROR")
    setProvenanceType("dual")
    val tripLines = sc.textFileProv(args(0), _.split(",")) //"datasets/commute/trips/part-000[0-4]*"
    try {
      val trips = tripLines.map { cols =>
        (cols(1), cols(3).toInt / cols(4).toInt)
      }
      val types = trips.map { s => 
        val speed = s._2
        if (_root_.monitoring.Monitors.monitorPredicate(speed > 40, (List[Any](speed), List[Any]()), 0)) {
          ("car", speed)
        } else if (_root_.monitoring.Monitors.monitorPredicate(speed > 15, (List[Any](speed), List[Any](speed)), 1)) {
          ("public", speed)
        } else {
          ("onfoot", speed)
        }
      }.collect().foreach(println)
//      val out = types.aggregateByKey((0.0d, 0))({
//        case ((sum, count), next) =>
//          (sum + next.value, count + 1)
//      }, {
//        case ((sum1, count1), (sum2, count2)) =>
//          (sum1 + sum2, count1 + count2)
//      }).mapValues({
//        case (sum, count) =>
//          sum.toDouble / count
//      }).collect()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
//    sco.stop()
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}
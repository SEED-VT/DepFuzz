package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance.setProvenanceType
import sparkwrapper.SparkContextWithDP
object CommuteType extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("CommuteType Monitored")
    val data1 = Array(",, ,0,1", ",, ,16,1", ",, ,41,1", " , , ,", " , , , ,0", " , , , ,", "", "", "", ",A, ,-0,1", ",A, ,-0,1")
    val data2 = Array(",Palms", ",Palms", ",Palms", "", "", "", "", ",", ",", "", "")
    val sco = new SparkContext(conf)
    val sc = new SparkContextWithDP(sco)
//    sc.setLogLevel("ERROR")
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
      }
      val out = types.aggregateByKey((0.0d, 0))({
        case ((sum, count), next) =>
          (sum + next.value, count + 1)
      }, {
        case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
      }).mapValues({
        case (sum, count) =>
          sum.toDouble / count
      }).take(100).foreach(println)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }


    _root_.monitoring.Monitors.finalizeProvenance()
  }
}
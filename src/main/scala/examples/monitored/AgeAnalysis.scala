package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object AgeAnalysis extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("AgeAnalysis")
    val sc = new SparkContextWithDP(new SparkContext(conf))

    sc.textFileProv(args(0), _.split(",")).map {
      cols => (cols(0), cols(1).toInt, cols(2).toInt)
    }.filter { s =>
      val bool = s._1.value.equals("90024")
      bool
    }.map {
      s =>
        if (_root_.monitoring.Monitors.monitorPredicate(s._2 >= 40 & s._2 <= 65, (List[Any](s._2), List[Any]()), 2)) {
          ("40-65", s._3)
        } else if (_root_.monitoring.Monitors.monitorPredicate(s._2 >= 20 & s._2 < 40, (List[Any](s._2), List[Any]()), 3)) {
          ("20-39", s._3)
        } else if (_root_.monitoring.Monitors.monitorPredicate(s._2 < 20, (List[Any](s._2), List[Any]()), 4)) {
          ("0-19", s._3)
        } else {
          (">65", s._3)
        }
    }
      .collect()
      .foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

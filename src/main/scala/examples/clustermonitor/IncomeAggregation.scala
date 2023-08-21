package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD.toPairRDD
import sparkwrapper.SparkContextWithDP

object IncomeAggregation extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("IncomeAggregation Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split(",")).map {
      cols => (cols(0), cols(1).toInt, cols(2).toInt)
    }.filter { s =>
      s._1.value.equals("90024")
    }.map {
      s =>
        if (_root_.monitoring.Monitors.monitorPredicate(s._2 >= 40 & s._2 <= 65, (List[Any](s._2), List[Any]()), 3)) {
          ("40-65", (s._3, 1))
        } else if (_root_.monitoring.Monitors.monitorPredicate(s._2 >= 20 & s._2 < 40, (List[Any](s._2), List[Any]()), 4)) {
          ("20-39", (s._3, 1))
        } else if (_root_.monitoring.Monitors.monitorPredicate(s._2 < 20, (List[Any](s._2), List[Any]()), 5)) {
          ("0-19", (s._3, 1))
        } else {
          (">65", (s._3, 1))
        }
    }
    toPairRDD(rdd)
      .mapValues(x => (x._2, x._2.toDouble))
      .take(100).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(x: (Int, Int), y: (Int, Int)): (Int, Int) = {
    (x._1 + y._1, x._2 + y._2)
  }
}

package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object NumberSeries {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("NumberSeries Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    sc.textFileProv(args(0), _.split(",")).map {
      s => s(1).toInt
    }.map { l =>
      var dis = 1
      var tmp = l
      if (_root_.monitoring.Monitors.monitorPredicate(l <= 0, (List[Any](l), List[Any]()), 1)) {
        dis = 0
      } else {
        while (tmp != 1 && dis < 30) {
          if (_root_.monitoring.Monitors.monitorPredicate(tmp % 2 == 0, (List[Any](tmp), List[Any]()), 2)) {
            tmp = (tmp / 2).toInt
          } else {
            tmp = tmp * 3 + 1
          }
          dis = dis + 1
        }
      }
      (l, dis)
    }.filter({
      case (l, m) =>
        m.equals(25)
    }).take(100).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

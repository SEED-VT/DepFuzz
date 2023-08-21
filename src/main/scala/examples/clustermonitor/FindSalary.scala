package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

object FindSalary {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("FindSalary Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val data = sc.textFileProv(args(0), _.split(",")).map(_ (0)).map {
      line =>
        if (_root_.monitoring.Monitors.monitorPredicate(line.value.substring(0, 1).equals("$"), (List[Any](line), List[Any]()), 1)) {
          line.substring(1, 6).toInt
        } else {
          line.toInt
        }
    }.filter { r =>
      r < 300
    }
      .reduce { (a, b) =>
        val sum = a + b
        sum
      }

    println(data)

    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

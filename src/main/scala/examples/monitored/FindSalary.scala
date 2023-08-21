package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.TaintedString

object FindSalary {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("FindSalary")
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
    }.collect()

    _root_.monitoring.Monitors.finalizeProvenance()
  }
}

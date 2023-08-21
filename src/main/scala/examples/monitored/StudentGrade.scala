package examples.monitored

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.TaintedInt

object StudentGrade {
  def main(args: Array[String]): ProvInfo = {
//    val conf = new SparkConf()
//    conf.setMaster("local[*]")
//    conf.setAppName("StudentGrade")
//    val sc = new SparkContextWithDP(new SparkContext(conf))
//    val rdd = sc.textFileProv(args(0), _.split(",")).map { a =>
//      val ret = (a(0), a(1).toInt)
//      ret
//    }.map { a =>
//      if (_root_.monitoring.Monitors.monitorPredicate(a._2 > 40, (List[Any](a._2), List[Any]()), 4)) (a._1 + " Pass", 1) else (a._1 + " Fail", 1)
//    }
//    val data = _root_.monitoring.Monitors.monitorReduceByKey(rdd, sum, 1).collect()
//
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: TaintedInt, b: TaintedInt): TaintedInt = {
    a + b
  }
}

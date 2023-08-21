package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.TaintedInt

object StudentGrade extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("StudentGrade Monitored")
    val sc = new SparkContextWithDP(new SparkContext(conf))
    val rdd = sc.textFileProv(args(0), _.split(",")).map { a =>
      val ret = (a(0), a(1).toInt)
      ret
    }.map { a =>
      if (_root_.monitoring.Monitors.monitorPredicate(a._2 > 40, (List[Any](a._2), List[Any]()), 4)) (a._1 + " Pass", 1) else (a._1 + " Fail", 1)
    }
      .map{case (a, b) => (a, b.asInstanceOf[Any])}

    _root_.monitoring.Monitors.monitorReduceByKey(rdd)(sum, 1)
      .take(100)
      .foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Any, b: Any): Int = {
    (a, b) match {
      case (x:Int, y:Int) => x + y
    }
  }
}

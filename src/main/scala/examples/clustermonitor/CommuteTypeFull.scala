package examples.clustermonitor

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.SymImplicits._
import fuzzer.ProvInfo

object CommuteTypeFull extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val conf = new SparkConf()
    if (args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(2))
    conf.setAppName("CommuteTypeFull Monitored")
    val sc = new SparkContextWithDP(SparkContext.getOrCreate(conf))
    sc.setLogLevel("ERROR")
    val trips = sc.textFileProv(args(0), s => s.split(",")).map {
      cols => (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
    }
    val locations = sc.textFileProv(args(1), s => s.filter(_ != '"').split(",")).map {
      cols => (cols(0), cols(3))
    }.filter {
      s => s._2.equals("Los Angeles")
    }
    val joined = _root_.monitoring.Monitors.monitorJoin(trips, locations, 1)
    val types = joined.map { s =>
      val speed = s._2._1
      if (_root_.monitoring.Monitors.monitorPredicate(speed > 40, (List[Any](speed), List[Any]()), 3)) {
        ("car", speed)
      } else if (_root_.monitoring.Monitors.monitorPredicate(speed > 15, (List[Any](speed), List[Any]()), 4)) {
        ("public", speed)
      } else {
        ("onfoot", speed)
      }
    }

    _root_.monitoring.Monitors.monitorReduceByKey(types)(sum, 2).take(100).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }
}
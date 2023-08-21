package examples.clustermonitor

import fuzzer.ProvInfo
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import sparkwrapper.SparkContextWithDP
import taintedprimitives.TaintedString
object Customers extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    val sparkConf = new SparkConf()
    if (args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
    sparkConf.setMaster(args(2))
    sparkConf.setAppName("Customers Monitored")
    val customers_data = args(0)
    val orders_data = args(1)
    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
    Provenance.setProvenanceType("dual")
    val customers = ctx.textFileProv(customers_data, _.split(',')) //.map(_.split(","))
    val orders = ctx.textFileProv(orders_data, _.split(',')) //.map(_.split(","))
    val o = orders.map({
      case Array(_, cid, date, iid) =>
        (cid, (iid, date.toInt))
    })
    val c = customers.map {
      row =>
        (row(0), row(1))
    }
    val joined = _root_.monitoring.Monitors.monitorJoin(c, o, 0).filter({
      case (_, (_, (_, date))) =>
        val this_year = 1641013200
        if (_root_.monitoring.Monitors.monitorPredicate(date > this_year, (List[Any](date, this_year), List[Any]()), 0)) true else false
    })
    val grouped = _root_.monitoring.Monitors.monitorGroupByKey(joined, 1)
    val numpur = grouped.mapValues {
      iter => iter.size
    }
    val thresh = numpur.filter(_._2 >= 3)
    val top = thresh.sortBy(_._2).take(3)
    if (_root_.monitoring.Monitors.monitorPredicate({
      top.length
    } < 3, (List[Any](), List[Any]()), 1)) {
      println("not enough data")
      return _root_.monitoring.Monitors.finalizeProvenance()
    }
    val rewards = top.map(computeRewards)
    rewards.foreach(println)

    println("customers")
    monitoring.Monitors.minData(0).foreach(println)

    println("orders")
    monitoring.Monitors.minData(1).foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }
  def computeRewards(custInfo: (TaintedString, Int)): (TaintedString, Float, String) = {
    val (id, num) = custInfo
    (id, 100.0f, s"$id has won ${"$"}100.0f")
  }
}
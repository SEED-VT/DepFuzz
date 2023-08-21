package transformers.testout
import org.apache.spark.{ SparkConf, SparkContext }, sparkwrapper.SparkContextWithDP, taintedprimitives._, taintedprimitives.SymImplicits._
object predicateTest2 {
  def main(args: Array[TaintedString]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
    val data1 = ctx.textFileProv("dummydata/predicateTest2", {
      arr => arr.split(",")
    })
    val data2 = ctx.textFileProv("dummydata/predicateTest2", {
      arr => arr.split(",")
    })
    val d1 = data1.map(arr => (arr.head, arr.tail))
    val d2 = data2.map(arr => (arr.head, arr.tail))
    val d3 = _root_.monitoring.Monitors.monitorJoin(d1, d2, 2).filter({
      case (k, (_, _)) =>
        k.toInt > 3 && k.toInt < 6
    })
    _root_.monitoring.Monitors.monitorGroupByKey(d3, 3).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
}
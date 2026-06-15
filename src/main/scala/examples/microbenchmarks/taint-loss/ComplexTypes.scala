package examples.microbenchmarks.taintloss

import org.apache.spark.{SparkConf, SparkContext}

/** Theoretical limitation: complex / user-defined types.
  *
  * User-defined classes expose arbitrary fields and methods. It is not feasible to define
  * complete, automatic propagation rules for every possible operation on every custom type.
  *
  * `SensorReading` wraps input columns; `fahrenheit` and `isAlert` apply ad-hoc logic that
  * no generic taint wrapper can fully model without hand-written rules per class.
  */
object ComplexTypes {

  case class SensorReading(site: String, celsius: Int) {
    def fahrenheit: Int = celsius * 9 / 5 + 32
    def isAlert: Boolean = fahrenheit > 100
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    conf.setAppName("ComplexTypes")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { cols => SensorReading(cols(0), cols(1).toInt) }
      .filter(_.isAlert)
      .map { r => (r.site, r.fahrenheit) }
      .take(10)
      .foreach(println)
  }
}

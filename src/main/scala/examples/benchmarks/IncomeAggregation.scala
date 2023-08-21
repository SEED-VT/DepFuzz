package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object IncomeAggregation extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
//    conf.setMaster(args(1))
    conf.setAppName("IncomeAggregation")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0))
      .map(_.split(","))
      .map {
      cols => (cols(0), cols(1).toInt, cols(2).toInt)
    }.filter { s =>
      s._1 == "90024"
    }.map {
      s =>
        if (s._2 >= 40 & s._2 <= 65) {
          ("40-65", (s._3, 1))
        } else if (s._2 >= 20 & s._2 < 40) {
          ("20-39", (s._3, 1))
        } else if (s._2 < 20) {
          ("0-19", (s._3, 1))
        } else {
          (">65", (s._3, 1))
        }
    }
    rdd
      .mapValues(x => (x._2, x._2.toDouble))
      .take(100).foreach(println)
  }

  def sum(x: (Int, Int), y: (Int, Int)): (Int, Int) = {
    (x._1 + y._1, x._2 + y._2)
  }
}

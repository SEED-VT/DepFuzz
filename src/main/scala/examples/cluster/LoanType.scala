package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object LoanType extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("LoanType Original")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { a =>
        (a(0).toFloat, a(1).toInt, a(2).toFloat, a(3))
      }
      .map { s =>
        var a = s._1
          for (i <- 1 to math.min(s._2, 100)) {
            a = a * (1 + s._3)
          }
        (a, s._2, s._3, s._4)
      }
      .take(100)
      .foreach(println)
  }
}
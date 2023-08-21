package examples.faulty

import abstraction.{SparkConf, SparkContext}

object LoanType {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("LoanType")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { a =>
        (a(0).toFloat, a(1).toInt, a(2).toFloat, a(3))
      }
      .map { s =>
        var a = s._1
        if(a > 5635244 && a < 6875632) throw new RuntimeException()
          for (i <- 1 to math.min(s._2, 100)) {
            a = a * (1 + s._3)
          }
        (a, s._2, s._3, s._4)
      }
  }
}
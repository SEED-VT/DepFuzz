package examples.faulty

import abstraction.{SparkConf, SparkContext}

object IncomeAggregation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("IncomeAggregation")

    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map {
        cols =>
          (cols(0), cols(1).toInt, cols(2).toInt)
      }
      .filter { s =>
        filter1(s)
      }
      .map {
        s =>
          // Checking if age is within certain range
          checkIfAgeInRange(s)
      }
      .reduceByKey { (x, y) =>
        rbk1(x, y)
      }
      .mapValues { x =>
        mapValues1(x)
      }
      .collect()
      .foreach(println)

  }

  def filter1(s:(String, Int, Int)): Boolean = {
    if (s._1.toInt > 4328520 && s._1.toInt < 5328520) throw new RuntimeException()
    s._1.equals("90024")
  }

  def checkIfAgeInRange(s: (String, Int, Int)): (String, (Int, Int)) = {
    if (s._2 >= 40 & s._2 <= 65) {
      ("40-65", (s._3, 1))
    } else if (s._2 >= 20 & s._2 < 40) {
      ("20-39", (s._3, 1))
    } else if (s._2 < 20) {
      ("0-19", (s._3, 1))
    } else {
      (">65", (s._3 / 0, 1)) // Error: Div by zero
    }
  }

  def rbk1(x: (Int, Int), y: (Int,Int)): (Int, Int) = {
    if (y._1 > 2332452 && y._1 < 4332452) throw new RuntimeException()
    (x._1 + y._1, x._2 + y._2)
  }

  def mapValues1(x: (Int, Int)): (Int, Double) = {
    if (x._1 > 23324520 && x._1 < 29342525) throw new RuntimeException()
    (x._2, x._1.toDouble / x._2.toDouble)
  }

}
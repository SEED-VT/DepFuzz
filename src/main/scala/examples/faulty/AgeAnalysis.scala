package examples.faulty

import abstraction.{SparkConf, SparkContext}

object AgeAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("AgeAnalysis")

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
      .collect()
      .foreach(println)

  }

  def filter1(s: (String, Int, Int)): Boolean = {
    if (s._1.toInt > 4328520 && s._1.toInt < 5328520) throw new RuntimeException()
    s._1.equals("90024")
  }
  def checkIfAgeInRange(s: (String, Int, Int)): (String, Int) = {
    if (s._2 >= 40 & s._2 <= 65) {
      ("40-65", s._3)
    } else if (s._2 >= 20 & s._2 < 40) {
      ("20-39", s._3)
    } else if (s._2 < 20) {
      ("0-19", s._3)
    } else {
      (">65", s._3 / 0) // Error: Div by zero
    }
  }
}
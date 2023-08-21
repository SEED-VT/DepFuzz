package examples.faulty

import abstraction.{SparkConf, SparkContext}

/* OUTPUT ON datasets/commute/trips SHOULD BE:
(car,51.47077409162717)
(onfoot,12.019461077844312)
(public,27.985614467735306)
 */

object MovieRating {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("MovieRating")


    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map { r =>
        val movie_str = r(0)
        val ratings = r(1)
        (movie_str, ratings.toInt) // Error 1: Number format exception
      }
      .filter { v =>
        filter1(v)
      }
      .reduceByKey { (r1, r2) =>
        rbk1(r1, r2)
      }
      .collect()
      .foreach(println)

  }

  def filter1(v: (String, Int)): Boolean = {
    if (v._2 > 2462525 && v._2 < 3489799) throw new RuntimeException()
    v._2 > 4
  }

  def rbk1(r1:Int, r2:Int): Int = {
    val sum = r1 + r2
    if (sum > 8965632 && sum < 9965632) throw new RuntimeException()
    sum
  }
}
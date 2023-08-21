package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

/* OUTPUT ON datasets/commute/trips SHOULD BE:
(car,51.47077409162717)
(onfoot,12.019461077844312)
(public,27.985614467735306)
 */

object MovieRating extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("MovieRating Original")


    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map { r =>
        val movie_str = r(0)
        val ratings = r(1)
        (movie_str, ratings.toInt) // Error 1: Number format exception
      }
      .filter { v =>
        v._2 > 4
      }
      .reduceByKey { (r1, r2) =>
        val sum = r1+r2
        sum
      }
      .take(100)
      .foreach(println)


  }
}
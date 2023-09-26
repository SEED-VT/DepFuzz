package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object MovieRating extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Movie Rating")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0)).map(_.split(",")).map { r =>
      val movie_str = r(0)
      val ratings = r(1)
      (movie_str, ratings.toInt)
    }.filter { v =>
      v._2 > 4
    }
      .map{case (a, b) => (a, b.asInstanceOf[Any])} // Temporary fix

    rdd.reduceByKey(sum)
      .take(100)
      .foreach(println)

  }

  def sum(a: Any, b: Any): Int = {
    (a, b) match {
      case (x: Int, y: Int) => x + y
    }
  }
}

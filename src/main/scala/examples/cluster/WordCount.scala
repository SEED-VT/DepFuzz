package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("WordCount Original")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split("\\s")) // "datasets/fuzzing_seeds/commute/trips"
      .map { s =>
        (s,1)
      }
      .reduceByKey { (a, b) =>
        val sum = a+b
        sum
      }// Numerical overflow
      .take(10)
      .foreach(println)

  }
}
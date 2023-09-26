package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.log10

object ExternalCall extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("External Call")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0))
      .map(_.split("\\s"))
      .flatMap(s => s)
      .map { s =>
        (s, 1)
      }
    rdd.reduceByKey((a, b) => a + b)
      .filter { v =>
        val v1 = log10(v._2)
        v1 > 1
      }.take(5)
      .foreach(println)
  }

}

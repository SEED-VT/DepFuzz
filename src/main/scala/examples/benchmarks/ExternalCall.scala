package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.log10

object ExternalCall extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    //    conf.setMaster(args(1))
    conf.setAppName("ExternalCall")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0))
      .map(_.split("\\s"))
      .flatMap(s => s)
      .map { s =>
        (s, 1)
      }
    rdd.reduceByKey(sum)
      .filter { v =>
        val v1 = log10(v._2)
        v1 > 1
      }.take(5)
      .foreach(println)
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }

}

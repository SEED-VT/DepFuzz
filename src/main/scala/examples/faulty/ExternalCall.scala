package examples.faulty

import abstraction.{SparkConf, SparkContext}

import scala.math.log10

object ExternalCall {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split("\\s")) // "datasets/fuzzing_seeds/commute/trips"
      .map { s =>
        map1(s)
      }
      .reduceByKey { (a, b) =>
        rbk1(a, b)
      }// Numerical overflow
      .filter{ v =>
        filter1(v)
      }
      .collect()
      .foreach(println)
  }

  def map1(s: String): (String, Int) = {
    if (s.startsWith("F3")) throw new RuntimeException()
    (s, 1)
  }
  def rbk1(a: Int, b: Int): Int = {
    val sum = a + b
    if (sum < 0) throw new RuntimeException()
    sum
  }

  def filter1(v:(String, Int)): Boolean = {
    val v1 = log10(v._2)
    if (v._1.startsWith("3")) throw new RuntimeException()
    v1 > 1
  }
}
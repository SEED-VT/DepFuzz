package examples.faulty

import abstraction.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split("\\s")) // "datasets/fuzzing_seeds/commute/trips"
      .map { s =>
        if(s.startsWith("F3")) throw new RuntimeException()
        (s,1)
      }
      .reduceByKey { (a, b) =>
        val sum = a+b
        if(sum < 0) throw new RuntimeException()
        sum
      }// Numerical overflow
      .collect()
      .foreach(println)
  }
}
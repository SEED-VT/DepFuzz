package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object StudentGrade extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Student Grade")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0)).map(_.split(",")).map { a =>
      val ret = (a(0), a(1).toInt)
      ret
    }.map { a =>
      if (a._2 > 40) (a._1 + " Pass", 1) else (a._1 + " Fail", 1)
    }
      .map{case (a, b) => (a, b)}

    rdd.reduceByKey((a, b) => a+b)
      .take(100)
      .foreach(println)

  }
}

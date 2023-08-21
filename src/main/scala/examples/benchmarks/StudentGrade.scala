package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object StudentGrade extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
//    conf.setMaster(args(1))
    conf.setAppName("StudentGrade")
    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.textFile(args(0)).map(_.split(",")).map { a =>
      val ret = (a(0), a(1).toInt)
      ret
    }.map { a =>
      if (a._2 > 40) (a._1 + " Pass", 1) else (a._1 + " Fail", 1)
    }
      .map{case (a, b) => (a, b.asInstanceOf[Any])}

    rdd.reduceByKey(sum)
      .take(100)
      .foreach(println)

    _root_.monitoring.Monitors.finalizeProvenance()
  }

  def sum(a: Any, b: Any): Int = {
    (a, b) match {
      case (x:Int, y:Int) => x + y
    }
  }
}

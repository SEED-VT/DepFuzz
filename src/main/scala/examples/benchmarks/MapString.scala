package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object MapString extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
//    conf.setMaster(args(1))
    conf.setAppName("MapString")

    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split("\n")).map { s =>
      s(0)
    }
      .take(100)
      .foreach(println)

  }
}
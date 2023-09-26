package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object MapString extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Map String")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split("\n")).map { s =>
      s(0)
    }
      .take(100)
      .foreach(println)

  }
}
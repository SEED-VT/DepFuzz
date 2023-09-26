package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Word Count")
    val ctx = SparkContext.getOrCreate(conf)
//    ctx.setLogLevel("ERROR")
    val ds = ctx.textFile(args(0)).map(_.split("\\s"))
      .flatMap(s => s)
      .map { s => (s, 1) }
    ds.reduceByKey((a,b) => a+b)
      .take(10)
      .foreach(println)
  }
}
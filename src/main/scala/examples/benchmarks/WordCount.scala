package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends Serializable {
  def main(args: Array[String]): Unit = {
    println(s"WordCount args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
//    sparkConf.setMaster(args(1))
    sparkConf.setAppName("WordCount")
    val ctx = SparkContext.getOrCreate(sparkConf)
//    ctx.setLogLevel("ERROR")
    val ds = ctx.textFile(args(0)).map(_.split("\\s"))
      .flatMap(s => s)
      .map { s => (s, 1) }
    ds.reduceByKey(sumFunc)
      .take(10)
      .foreach(println)
  }

  def sumFunc(a: Int, b: Int): Int = {
    a + b
  }
}
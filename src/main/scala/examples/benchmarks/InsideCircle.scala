package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object InsideCircle extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    //    conf.setMaster(args(1))
    conf.setAppName("InsideCircle")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(",")).map {
      cols => (cols(0).toInt, cols(1).toInt, cols(2).toInt)
    }.filter {
      case (x, y, z) =>
        x * x + y * y < z * z
    }.take(100).foreach(println)
  }
}

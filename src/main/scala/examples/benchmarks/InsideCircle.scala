package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object InsideCircle extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Inside Circle")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(",")).map {
      cols => (cols(0).toInt, cols(1).toInt, cols(2).toInt)
    }.filter {
      case (x, y, z) =>
        x * x + y * y < z * z
    }.take(100).foreach(println)
  }
}

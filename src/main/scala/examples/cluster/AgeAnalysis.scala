package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object AgeAnalysis extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("AgeAnalysis Original")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")).map {
      cols => (cols(0), cols(1).toInt, cols(2).toInt)
    }.filter { s =>
      val bool = s._1.equals("90024")
      bool
    }.map {
      s =>
        if (s._2 >= 40 & s._2 <= 65) {
          ("40-65", s._3)
        } else if (s._2 >= 20 & s._2 < 40) {
          ("20-39", s._3)
        } else if (s._2 < 20) {
          ("0-19", s._3)
        } else {
          (">65", s._3)
        }
    }
      .take(10)
      .foreach(println)

  }
}

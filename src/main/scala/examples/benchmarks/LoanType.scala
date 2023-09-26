package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object LoanType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Loan Type")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map {
        a => (a(0).toFloat, a(1).toInt, a(2).toFloat, a(3))
      }.map { s =>
        var a = s._1
        val upper = math.min(s._2, 30)
        for (i <- 1 to upper) {
          a = a * (1 + s._3)
        }
        (a, s._2, s._3, s._4)
      }
      .take(100)
      .foreach(println)
  }
}

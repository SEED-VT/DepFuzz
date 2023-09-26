package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object FindSalary {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if (args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Find Salary")
    val sc = SparkContext.getOrCreate(conf)
    val data = sc.textFile(args(0))
      .map(_.split(","))
      .map(_(0))
      .map {
        line =>
          if (line.substring(0, 1).equals("$")) {
            line.substring(1, 6).toInt
          } else {
            line.toInt
          }
      }
    val filtered = data.filter { r =>
      r < 300
    }
    filtered.reduce { (a, b) =>
      val sum = a + b
      sum
    }

    println(data)

  }
}

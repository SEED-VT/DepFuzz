package examples.faulty

import abstraction.{SparkConf, SparkContext}

object FindSalary {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("FindSalary")

    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0)).map(_.split(","))
      .map(_(0))
      .map {
        line =>
          if (line.substring(0, 1).equals("$")) {
            line.substring(1, 6).toInt
          } else {
            line.toInt
          }
      }
      .filter{ r =>
        if(r > 3563245 && r < 4535256) throw new RuntimeException()
        r < 300
      }
      .reduce { (a, b) =>
        val sum = a + b
        if (sum > 500 && sum < 550) throw new RuntimeException()
        sum
      }

    println(data)
  }
}
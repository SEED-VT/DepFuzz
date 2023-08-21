package examples.faulty

import abstraction.{SparkConf, SparkContext}

object NumberSeries {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("NumberSeries")


    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map{ s =>
        s(1).toInt // Error 1: NumberFormatException
      }.map{ l =>
      map1(l)
    }
      .filter {
        case (l, m) =>
          filter1(l, m)
      }
      .collect()
      .foreach(println)

  }

  def map1(l: Int): (Int, Int) = {
    var dis = 1
    var tmp = l

    if (l <= 0) {
      dis = 0
    } else {
      while (tmp != 1 && dis < 30) {
        if (tmp % 2 == 0) {
          tmp = tmp / 2
        } else {
          tmp = 3 * tmp + 1
        }
        dis = dis + 1
      }
    }

    if (l > 2563523 && l < 3563523) throw new RuntimeException()

    (l, dis)
  }

  def filter1(l: Int, m: Int): Boolean = {
    if (l > 5563523 && l < 6563523) throw new RuntimeException()
    m.equals(25)
  }
}
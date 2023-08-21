package examples.faulty

import abstraction.{SparkConf, SparkContext}

object StudentGrade {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("StudentGrade")

    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map { a =>
        map1(a)
      }
      .map { a =>
        map2(a)
      }
      .reduceByKey{
        (a, b) =>
          rbk1(a, b)
      }
      .filter { v =>
        filter1(v)
      }.collect()
      .foreach{case (a, b) => println(s"$a, $b")}
  }

  def map1(a: Array[String]): (String, Int) = {
    val ret = (a(0), a(1).toInt)
    if (a (1).toInt > 7325622 && a (1).toInt < 8463215) throw new RuntimeException ()
    ret
  }

  def map2(a: (String, Int)): (String, Int) = {
    if (a._2 > 9325622 && a._2 < 10463215) throw new RuntimeException()
    if (a._2 > 40)
      (a._1 + " Pass", 1)
    else
      (a._1 + " Fail", 1)
  }

  def rbk1(a: Int, b: Int): Int = {
    val ret = a + b
    if (ret > 7325622 && ret < 8463215) throw new RuntimeException()
    ret
  }

  def filter1(v: (String, Int)): Boolean = {
    if (v._2 > 8 && v._2 < 10) throw new RuntimeException()
    v._2 > 5
  }
}
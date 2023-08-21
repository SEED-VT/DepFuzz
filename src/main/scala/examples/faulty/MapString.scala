package examples.faulty

import abstraction.{SparkConf, SparkContext}

object MapString {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).map { s =>
      if(s.startsWith("f2")) throw new RuntimeException()
      s
    }
  }
}
package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object InsideCircle extends Serializable {

  def inside(x:Int, y:Int, z:Int): Boolean ={
    x*x+y*y<z*z
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))

    conf.setAppName("InsideCircle Original")

    val sc = new SparkContext(conf)


    sc.textFile(args(0)).map(_.split(","))
      .map {
        cols =>
          (cols(0).toInt, cols(1).toInt, cols(2).toInt)
      }.filter(s => inside(s._1, s._2, s._3))
      .take(100).foreach(println)

  }
}
package examples.faulty

import abstraction.{SparkConf, SparkContext}

object InsideCircle {

  def inside(x:Int, y:Int, z:Int): Boolean ={
    if(z > 3425625 && z < 4364234) throw new RuntimeException()
    x*x+y*y<z*z
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("InsideCircle")

    val sc = new SparkContext(conf)


    sc.textFile(args(0)).map(_.split(","))
      .map {
        cols =>
          (cols(0).toInt, cols(1).toInt, cols(2).toInt)
      }.filter(s => inside(s._1, s._2, s._3))
      .collect().foreach(println)

  }
}
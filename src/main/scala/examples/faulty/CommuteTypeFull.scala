package examples.faulty

import abstraction.{SparkConf, SparkContext}

/* OUTPUT ON datasets/commute/trips SHOULD BE:
(car,51.47077409162717)
(onfoot,12.019461077844312)
(public,27.985614467735306)
 */

object CommuteTypeFull {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("CommuteTime")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val trips = sc.textFile(args(0))
      .map { s =>
        val cols = s.split(",")
        (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
      }
    val locations = sc.textFile(args(1))
      .map { s =>
        val sfil = s.filter(_ != '\"')
        val cols = sfil.split(",")
        (cols(0), cols(3))
      }
      .filter{
        s =>
          if(s._2.startsWith("s4")) throw new RuntimeException()
          s._2.equals("Los Angeles")
      }

    val joined = trips.join(locations)
    joined
      .map { s =>
        // Checking if speed is < 25mi/hr
        map1(s)
      }
      .reduceByKey{
        case (a, b) =>
          rbk1(a, b)
      }
      .collect
      .foreach(println)

  }

  def map1(tup: (String, (Int, String))): (String, Int) = {
    val speed = tup._2._1
    if (speed < 0) { throw new RuntimeException() }
    if (speed > 40) {
      ("car", speed)
    } else if (speed > 15) {
      ("public", speed)
    } else {
      ("onfoot", speed)
    }
  }

  def rbk1(a: Int, b: Int): Int = {
    if (a > 5675864 && a < 6987456)  throw new RuntimeException()
    a + b
  }
}
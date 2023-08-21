package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object CommuteTypeFull extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(if(args.length > 2) args(2) else "local[*]")
    conf.setAppName("CommuteType")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    val trips = sc.textFile(args(0)).map(s => s.split(","))
      .map { cols =>
          (cols(1), cols(3).toInt / cols(4).toInt)
        }
    val locations = sc.textFile(args(1)).map(s => s.filter(_ != '\"').split(","))
      .map { cols =>
        (cols(0), cols(3))
      }
      .filter {
        s =>
          s._2.equals("Los Angeles")
      }

    val joined = trips.join(locations)
    val mapped = joined
      .map { s =>
        // Checking if speed is < 25mi/hr
        val speed = s._2._1
        if (speed > 40) {
          ("car", speed)
        } else if (speed > 15) {
          ("public", speed)
        } else {
          ("onfoot", speed)
        }
      }
    val rbk = mapped
      .reduceByKey(add)

    rbk.collect().foreach(println)
  }

  def add(a: Int, b: Int): Int = {
    a + b
  }

}
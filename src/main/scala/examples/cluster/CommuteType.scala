package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object CommuteType extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("CommuteType Original")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
    val tripLines = sc.textFile(args(0)).map(_.split(",")) //"datasets/commute/trips/part-000[0-4]*"
    try {
      val trips = tripLines.map { cols =>
        (cols(1), cols(3).toInt / cols(4).toInt)
      }
      val types = trips.map { s => 
        val speed = s._2
        if (speed > 40) {
          ("car", speed)
        } else if (speed > 15) {
          ("public", speed)
        } else {
          ("onfoot", speed)
        }
      }
      val out = types.aggregateByKey((0.0d, 0))({
        case ((sum, count), next) =>
          (sum + next, count + 1)
      }, {
        case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
      }).mapValues({
        case (sum, count) =>
          sum.toDouble / count
      }).take(100).foreach(println)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}
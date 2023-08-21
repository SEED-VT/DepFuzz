package examples.fuzzable

import abstraction.{SparkConf, SparkContext}

/* OUTPUT ON datasets/commute/trips SHOULD BE:
(car,51.47077409162717)
(onfoot,12.019461077844312)
(public,27.985614467735306)
 */

object CommuteType {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("CommuteType")


    val sc = new SparkContext(conf)

    val tripLines = sc.textFile(args(0)) // "datasets/fuzzing_seeds/commute/trips"
      val trips = tripLines
        .map { s =>
          val cols = s.split(",")
          (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
        }
      val types = trips
        .map { s =>
          val speed = s._2
          if (speed > 40) {
            ("car", speed)
          } else if (speed > 15) {
            ("public", speed)
          } else {
            ("onfoot", speed)
          }
        }

      //val out = AggregationFunctions.sumByKey(types)// types.reduceByKey(_ + _)
      // other functions to consider: intstreaming
      val out = types.aggregateByKey((0.0, 0))(
        { case ((sum, count), next) => (sum + next, count + 1) },
        { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
      ).mapValues({ case (sum, count) => sum.toDouble / count }).collect().take(10).foreach(println)
  }
}
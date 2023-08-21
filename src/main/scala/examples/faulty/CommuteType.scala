package examples.faulty

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

    val tripLines = sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      val trips = tripLines
        .map { cols =>
          (cols(1), cols(3).toFloat / cols(4).toFloat)
        }
      val types = trips
        .map { s =>
          val speed = s._2
          if(speed <= 0) {
            throw new RuntimeException()
//            ("error", 1.0f)
          } else if (speed > 40) {
            ("car", speed)
          } else if (speed > 15) {
            ("public", speed)
          } else {
            ("onfoot", speed)
          }
        }

      //val out = AggregationFunctions.sumByKey(types)// types.reduceByKey(_ + _)
      // other functions to consider: intstreaming
      types.aggregateByKey((0.0, 0))(
        { case ((sum, count), next) =>
          if (sum > 2456573 && sum < 3456573) throw new RuntimeException()
          (sum + next, count + 1)
        },
        { case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
        }
      ).mapValues({
        case (sum, count) =>
          if(sum > 10.0 && sum < 14.0) throw new RuntimeException()
          sum / count
      }).collect().take(10).foreach(println)

  }
}
package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object NumberSeries extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(1))
    conf.setAppName("NumberSeries Original")


    val sc = new SparkContext(conf)

    sc.textFile(args(0)).map(_.split(",")) // "datasets/fuzzing_seeds/commute/trips"
      .map{ s =>
        s(1).toInt // Error 1: NumberFormatException
      }.map{ l =>
      var dis = 1
      var tmp = l

      if(l <= 0){
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


      (l, dis)
    }
      .filter {
        case (l, m) =>
          m.equals(25)
      }
      .take(100)
      .foreach(println)


  }
}
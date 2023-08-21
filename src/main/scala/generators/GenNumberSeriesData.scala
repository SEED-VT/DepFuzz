import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenNumberSeriesData extends Serializable {

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt( (max - min) + 1 )
  }

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/numbers"),
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: NumberSeries")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )
    //    val fault_rate = 0.0001
    //    def faultInjector()  = if(Random.nextInt(dataper*partitions) < dataper*partitions* fault_rate) true else false

    datasets.foreach { case (_, f) =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // 32,234
          val n1 = randIntBetween(1, 500)
          val n2 = randIntBetween(1, 500)
          s"""$n1,$n2"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
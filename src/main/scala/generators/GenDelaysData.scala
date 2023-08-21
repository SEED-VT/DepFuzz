import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenDelaysData extends Serializable {

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/station1"),
      ("ds2", s"hdfs://zion-headnode:9000/ahmad/$name/station2")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: Delays")

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
          // trip58490,95880023,68370870,route3
          val tripId = s"trip${Random.nextInt(99999)}"
          val a = Random.nextInt(Int.MaxValue)
          val d = Random.nextInt(Int.MaxValue)
          val r = s"route${Random.nextInt(100)}"
          s"""$tripId,$a,$d,$r"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
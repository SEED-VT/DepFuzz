import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenDeliveryFaultsData extends Serializable {

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
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/deliveries")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: DeliveryFaults")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )
    //    val fault_rate = 0.0001
    //    def faultInjector()  = if(Random.nextInt(dataper*partitions) < dataper*partitions* fault_rate) true else false

    datasets.foreach { case (ds, f) =>
          SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
            (1 to dataper).map { _ =>
              // dlvry58490,cust44067,vend57550,5
              val did = s"dlvry${Random.nextInt(99999)}"
              val cid = s"cust${Random.nextInt(99999)}"
              val vend = s"vend${Random.nextInt(99999)}"
              val rating = Random.nextInt(5)+1
              s"""$did,$cid,$vend,$rating"""
            }.iterator
          }.saveAsTextFile(f)

    }
  }

}
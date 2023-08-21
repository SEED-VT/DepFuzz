import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenSegmentationData extends Serializable {

  def generateURL(len: Int): String = {
    s"www.${Random.alphanumeric.take(len).mkString}.com"
  }

  def getRandomComponentType: String = {
    val types = Array(
      "advertisement",
      "header",
      "footer",
      "heading",
      "logo"
    )
    types(Random.nextInt(types.length))
  }

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val url_len = 2
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("before", s"hdfs://zion-headnode:9000/ahmad/$name/before"),
      ("after", s"hdfs://zion-headnode:9000/ahmad/$name/after")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: Webpage Segmentation")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |url len: $url_len
         |seed: $seed
         |""".stripMargin
    )
    //    val fault_rate = 0.0001
    //    def faultInjector()  = if(Random.nextInt(dataper*partitions) < dataper*partitions* fault_rate) true else false

    datasets.foreach { case (_, f) =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          val url = generateURL(url_len)
          val swx = Random.nextInt(1920)
          val swy = Random.nextInt(1080)
          val w = Random.nextInt(1920)
          val h = Random.nextInt(1080)
          val cid = Random.nextInt(10)
          val ctype = getRandomComponentType
          s"""$url,$swx,$swy,$h,$w,$cid,$ctype"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
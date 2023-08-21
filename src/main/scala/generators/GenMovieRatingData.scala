import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenMovieRatingData extends Serializable {

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
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/ratings"),
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: MovieRating")

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
          // ASTAR,2
          val movie = generateString(10)
          val rating = randIntBetween(0, 100)
          s"""$movie,$rating"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
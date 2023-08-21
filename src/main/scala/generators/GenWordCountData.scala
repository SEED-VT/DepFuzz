import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object GenWordCountData extends Serializable {


  def main(args: Array[String]): Unit = {

    val partitions = args(0).toInt // e.g. 200
    val dataper = args(1).toInt // e.g. 100000
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/words")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: WordCount")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, f) =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          s"This is a sentence"
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
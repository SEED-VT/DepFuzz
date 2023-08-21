import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenFindSalaryData extends Serializable {

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt( (max - min) + 1 )
  }

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/salaries"),
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: FindSalary")

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
          // 1000
          val salary = randIntBetween(100, 999999)
          val dollar = if(Random.nextBoolean() && salary > 99999) "$" else ""
          s"""$dollar$salary"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
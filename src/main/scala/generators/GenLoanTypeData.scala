import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenLoanTypeData extends Serializable {

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt( (max - min) + 1 )
  }

  def randFloatBetween(min: Int, max: Int): Float = {
    (randIntBetween(min*math.pow(10.0, 4).toInt, max*math.pow(10.0, 4).toInt)/math.pow(10.0, 4)).toFloat
  }

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/info")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: LoanType")

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
              // 3400.0,40,0.0211,Amy
              val id = randIntBetween(1000, 9999)
              val years = randIntBetween(10, 50)
              val rate = randFloatBetween(0, 1)
              val name = generateString(10)
              s"""$id,$years,$rate,$name"""
            }.iterator
          }.saveAsTextFile(f)
    }
  }

}
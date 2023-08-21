import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object GenCommuteTypeData extends Serializable {

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
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/trips")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: CommuteType")

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
              def zipcode = "9" + "0"+ "0" + Random.nextInt(10).toString + Random.nextInt(10).toString
              val z1 = zipcode
              val z2 = zipcode
              val dis = Math.abs(z1.toInt - z2.toInt)*100 +  Random.nextInt(10)
              var velo = Random.nextInt(70)+3
              if( velo <= 10){
                if(zipcode.toInt % 100 != 1){
                  velo = velo+10
                }
              }
              var time = dis/(velo)
              time  = if(time ==0) 1 else time

              s"""sr,$z1,$z2,$dis,$time"""
            }.iterator
          }.saveAsTextFile(f)
    }
  }

}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object GenFlightData extends Serializable {

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt( (max - min) + 1 )
  }

  def randFloatBetween(min: Int, max: Int): Float = {
    (randIntBetween(min*math.pow(10.0, 4).toInt, max*math.pow(10.0, 4).toInt)/math.pow(10.0, 4)).toFloat
  }


  def getAirportCode: String ={
    val arr = Array("LAX" , "SFO" , "JFK" , "ORD" , "MDW" , "SEA" , "SJC" , "BNA" , "LGA" , "DAL" , "FTW" , "PHX" , "BUR" , "JAX" , "ATL" , "MNN"
    , "KOX", "OAK" , "RNO" , "ANC" , "MIA" , "MCO" , "BOS" , "DTW" , "MSP" , "EWR" , "ROC" , "SYR" , "CLE" , "PDX" , "PHL" , "PVD" , "HOU" , "SLC",
     "MSN" , "MKE" , "LHR" , "LHE" , "IST" , "ISB" , "RYD" , "DBX" , "ADU" , "FRK"  , "FRN" , "IRN" , "JAP" , "SUL" , "POL" , "PUP" , "SYX",
     "MLB" , "PRT" , "MNA" , "MUX" , "MLA" , "SPB" , "MOS" , "CAR" , "CUZ" , "RDJ" , "SPO" , "OCA" , "LBG" , "BUB" , "LAK" , "LUT" , "XYK" ,
     "ZUT" , "AUZ" , "AUX" , "ZUN" , "ZXA" , "NPW" , "NBA" , "NVM" , "PNA" , "EWQ" , "QWS" , "QRD"  , "LAS" , "NOW" , "WER" , "WRT" , "WPO")

    val one = Random.nextInt(arr.length)
    arr(one)
  }

  def main(args: Array[String]): Unit = {
    val partitions = args(0).toInt
    val dataper = args(1).toInt
    val name = s"${args(2)}_${partitions*dataper}"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", s"hdfs://zion-headnode:9000/ahmad/$name/airports"),
      ("ds2", s"hdfs://zion-headnode:9000/ahmad/$name/flights")
    )
    sparkConf.setMaster("spark://zion-headnode:7077")
    sparkConf.setAppName("DataGen: FlightDistance")

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
      ds match {
        case "ds1" =>
          SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
            (1 to dataper).map { _ =>
              // KGD,"Khrabrovo Airport",Kaliningrad,20.5925998687744,54.8899993896484,Europe/Kaliningrad
              val airportCode = getAirportCode
              val airportName = generateString(10)
              val airportName2 = generateString(10)
              val long = randFloatBetween(-180, 180)
              val lat = randFloatBetween(-90, 90)
              val continent = generateString(10)
              s"""$airportCode,$airportName,$airportName2,$long,$lat,$continent"""
            }.iterator
          }.saveAsTextFile(f)

        case "ds2" =>
          SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
            (1 to dataper).map { _ =>
              // 32658,PG0674,"2017-08-19 02:35:00.000","2017-08-19 05:00:00.000",KRO,KJA,Scheduled,CR2,,
              val fid = Random.nextInt(99999)
              val pgid = s"PG${randIntBetween(1000, 9999)}"
              val arrivalTime = s"20${randIntBetween(15,22)}-${randIntBetween(1,12)}-${randIntBetween(1, 28)} ${randIntBetween(0,23)}:${randIntBetween(0,60)}"
              val depTime = s"20${randIntBetween(15,22)}-${randIntBetween(1,12)}-${randIntBetween(1, 28)} ${randIntBetween(0,23)}:${randIntBetween(0,60)}"
              val arrivalAirport = getAirportCode
              val depAirport = getAirportCode
              val status = Array("scheduled", "departed", "completed").apply(Random.nextInt(3))
              val idk = s"CR${randIntBetween(1,10)}"
              s"""$fid,$pgid,$arrivalTime,$depTime,$arrivalAirport,$depAirport,$status,$idk"""
            }.iterator
          }.saveAsTextFile(f)
      }
    }
  }

}
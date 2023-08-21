package examples.motivating

import abstraction.{SparkConf, SparkContext}
import org.apache.commons.math3.exception.NotANumberException
import utils.MutationUtils.flipCoin

object FlightDistance {

  def main(args: Array[String]): Unit = {
    println(s"synthetic 3 args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Column Provenance Test").set("spark.executor.memory", "2g")
    val flights_data = args(0) //"datasets/fuzzing_seeds/FlightDistance/flights"
    val airports_data = args(1) //"datasets/fuzzing_seeds/FlightDistance/airports_data"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val flights = ctx.textFile(flights_data).map(_.split(','))
    val airports = ctx.textFile(airports_data).map(_.split(','))


    val airports_and_coords = airports
      .map(r => (r(0), (r(3), r(4))))

    val aflights_and_coords = flights
      .map(r => (r(5), r(0)))
      .join(airports_and_coords)
      .map{case (ap, (id, (lat, long))) => (id, (ap, lat, long))}

    val flights_and_distances = flights
      .map(r => (r(4), r(0)))
      .join(airports_and_coords)
      .map{case (ap, (id, (lat, long))) => (id, (ap, lat, long))}
      .join(aflights_and_coords)
      .map{
        case (fid, ((dap, dlat, dlong), (aap, alat, along))) =>
          (fid, (dap,aap,distance((dlat.toFloat,dlong.toFloat),(alat.toFloat,along.toFloat))))
      }


    flights_and_distances.collect().take(10).foreach(println)
  }
  def distance(departure: (Float, Float), arrival: (Float, Float)): Float = {
    val R = 6373.0
    val (dlat, dlong) = departure
    val (alat, along) = arrival
    val (dlatr, dlongr) = (toRad(dlat), toRad(dlong))
    val (alatr, alongr) = (toRad(alat), toRad(along))
    val difflat = alatr-dlatr
    val difflong = alongr-dlongr

    val a = math.pow(math.sin(difflat / 2), 2) + math.cos(dlatr) * math.cos(alatr) * math.pow(math.sin(difflong / 2),2)
    val c = 2*math.atan2(math.sqrt(a), math.sqrt(.1-a)) // should be 1-a. Results in NaNs
    val dist = (R * c * 0.621371).toFloat
    if(dist.isNaN) throw new NotANumberException()
    dist
  }

  def toRad(d: Float): Float = {
    (d * math.Pi/180.0).toFloat
  }

}
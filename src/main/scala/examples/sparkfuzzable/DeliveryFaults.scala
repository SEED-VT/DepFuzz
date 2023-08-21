package examples.sparkfuzzable

import org.apache.spark.{SparkConf, SparkContext}

object DeliveryFaults {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Delivery Faults")
    val sc = new SparkContext(conf)

    //<delivery_id>,<customer_id>,<vendor>,<rating>
    val deliveries = sc.textFile(args(0))
      .map(_.split(','))
      .map(r => (r(0), (r(1), r(2), r(3).toFloat)))


    val same_deliveries = deliveries.groupByKey()
    val triplets = same_deliveries.filter(_._2.size > 2)
    val bad_triplets = triplets.filter(tup => tripletRating(tup) < 2.0f)
    bad_triplets
      .map(processTriplets)
      .collect()
      .foreach(println)

  }

  def tripletRating(tup: (String, Iterable[(String, String, Float)])): Float = {
    val (_, iter) = tup
    iter.foldLeft(0.0f){case (acc, (_, _, rating)) => rating + acc}/iter.size
  }

  def processTriplets(tup: (String, Iterable[(String, String, Float)])): String = {
    val (_, iter) = tup
    iter.foldLeft(""){case (acc,(_, vendor,_)) => s"$acc,$vendor"}
  }
}
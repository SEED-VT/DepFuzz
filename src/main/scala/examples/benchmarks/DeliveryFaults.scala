package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object DeliveryFaults extends Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(if(args.length > 1) args(1) else "local[*]")
    conf.setMaster("local[*]")
    conf.setAppName("Delivery Faults")
    val sc = SparkContext.getOrCreate(conf)

    //<delivery_id>,<customer_id>,<vendor>,<rating>
    val deliveries = sc.textFile(args(0))
      .map(_.split(','))
      .map(r => (r(0), (r(1), r(2), r(3).toFloat)))


    val same_deliveries = deliveries.groupByKey()
    val triplets = same_deliveries.filter(_._2.size > 2)
    val bad_triplets = triplets.filter(tup => tripletRating(tup) < 2.0f)
    bad_triplets
      .map {
        case (_, iter) =>
          iter.foldLeft("")({
            case (acc, (_, vendor, _)) =>
              s"$acc,$vendor"
          })
      }
      .collect()
      .foreach(println)
  }

  def tripletRating(tup: (String, Iterable[(String, String, Float)])): Float = {
    val (_, iter) = tup
    iter.foldLeft(0.0f){case (acc, (_, _, rating)) => rating + acc}/iter.size
  }

}
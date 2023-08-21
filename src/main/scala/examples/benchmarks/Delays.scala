package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object Delays extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(if(args.length > 2) args(2) else "local[*]")
    conf.setAppName("Bus Delays")
    val sc = SparkContext.getOrCreate(conf)

    //<id>,<departure_time>,<advertised_departure>
    val station1 = sc.textFile(args(0))
      .map(_.split(','))
      .map(r => (r(0), (r(1).toInt, r(2).toInt, r(3))))

    //<id>,<arrival_time>,<advertised_arrival>
    val station2 = sc.textFile(args(1))
      .map(_.split(','))
      .map(r => (r(0), (r(1), r(2), r(3))))

    val joined = station1
      .join(station2)
    val mapped = joined
      .map{case (_, ((dep, adep, rid), (arr, aarr, _))) => (buckets((arr.toInt-aarr.toInt) - (dep-adep)), rid)} //bug idea, don't cater for early arrivals
    val grouped = mapped.groupByKey()
    val filtered = grouped
      .filter(_._1 > 2) // filter delays more than an hour
      .flatMap(_._2)
      .map((_, 1))

    val reduced = filtered
      .reduceByKey{case (a, b) => a+b}

    reduced
      .collect()
      .foreach(println)
  }

  def buckets(v: Int): Int = {
    v / 1800 // groups of 30 min delays
  }
}
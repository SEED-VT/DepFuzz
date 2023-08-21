package examples.fuzzable

import abstraction.{SparkConf, SparkContext}

object Delays {


  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Bus Delays")
    val sc = new SparkContext(conf)

    //<id>,<departure_time>,<advertised_departure>
    val station1 = sc.textFile(args(0))
      .map(_.split(','))
      .map(r => (r(0), (r(1).toLong, r(2).toLong, r(3))))

    //<id>,<arrival_time>,<advertised_arrival>
    val station2 = sc.textFile(args(1))
      .map(_.split(','))
      .map(r => (r(0), (r(1).toLong, r(2).toLong, r(3))))

    station1
      .join(station2)
      .map{case (_, ((dep, adep, rid), (arr, aarr, _))) => (buckets((arr-aarr) - (dep-adep)), rid)} //bug idea, don't cater for early arrivals
      .groupByKey()
      .filter(filter1) // filter delays more than an hour
      .flatMap(flatMap1)
      .map(map1)
      .reduceByKey(rbk1)
      .collect()
      .foreach(println)
  }

  def buckets(v: Long): Long = v / 1800 // groups of 30 min delays
  def filter1(tup: (Long, Any)): Boolean = if(tup._1 > 2) true else false
  def flatMap1(s: (Long, Seq[String])) = s._2
  def map1(s: String) = (s, 1)
  def rbk1(a: Int, b: Int) = a+b

}
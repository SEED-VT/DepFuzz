package examples.cluster

import org.apache.spark.{SparkConf, SparkContext}

object Delays extends Serializable {


  def main(args: Array[String]) {
    val conf = new SparkConf()
    if(args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
    conf.setMaster(args(2))
    conf.setAppName("Delays Original")
    val sc = new SparkContext(conf)

    //<id>,<departure_time>,<advertised_departure>
    val station1 = sc.textFile(args(0))
      .map(_.split(','))
      .map(r => (r(0), (r(1).toInt, r(2).toInt, r(3))))

    //<id>,<arrival_time>,<advertised_arrival>
    val station2 = sc.textFile(args(1))
      .map(_.split(','))
      .map(r => (r(0), (r(1), r(2), r(3))))

    station1
      .join(station2)
      .map{case (_, ((dep, adep, rid), (arr, aarr, _))) => (buckets((arr.toInt-aarr.toInt) - (dep-adep)), rid)} //bug idea, don't cater for early arrivals
      .groupByKey()
      .filter(filter1) // filter delays more than an hour
      .flatMap(flatMap1)
      .map(map1)
      .reduceByKey(rbk1)
      .take(10)
      .foreach(println)
  }

  def buckets(v: Int): Int = v / 1800 // groups of 30 min delays
  def filter1(tup: (Int, Iterable[String])): Boolean = {
    if(tup._1 > 2) true else false
  }
  def flatMap1(s: (Int, Iterable[String])) = {
    s._2
  }
  def map1(s: String) = {
    (s, 1)
  }
  def rbk1(a: Int, b: Int) = {
    a+b
  }

}
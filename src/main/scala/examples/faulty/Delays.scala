package examples.faulty

import abstraction.{SparkConf, SparkContext}
import org.w3c.dom.ranges.RangeException

object Delays {


  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Bus Delays")
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
      .collect()
      .foreach(println)
  }

  def buckets(v: Int): Int = v / 1800 // groups of 30 min delays
  def filter1(tup: (Int, Any)): Boolean = {
    if(tup._1 > 1240690 && tup._1 < 2942678) throw new RuntimeException() // synthetic bug
    if(tup._1 > 2) true else false
  }
  def flatMap1(s: (Int, Seq[String])) = {
    if(s._1 > 5823574 && s._1 < 6903842) throw new RuntimeException() // synthetic bug
    s._2
  }
  def map1(s: String) = {
    if(s.substring(1, 3).equals("#%")) throw new RuntimeException() // synthetic bug
    (s, 1)
  }
  def rbk1(a: Int, b: Int) = {
    if(a >= 4) throw new RuntimeException() // synthetic bug
    a+b
  }

}
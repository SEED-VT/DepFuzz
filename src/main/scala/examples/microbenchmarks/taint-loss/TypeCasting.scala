package examples.microbenchmarks.taintloss

import org.apache.spark.{SparkConf, SparkContext}

/** Theoretical limitation: type casting.
  *
  * Taint may be recoverable for specific cast sites, but it is impossible to automatically
  * detect and repair every `asInstanceOf` (or equivalent cast) in general.
  *
  * Here a tainted column value is erased to `Any` and downcast again; no generic scheme can
  * know which provenance to attach to the result across all possible cast targets.
  */
object TypeCasting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    conf.setAppName("TypeCasting")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { cols => (cols(0), cols(1).toInt) }
      .map { case (key, value) =>
        val erased: Any = value
        val restored = erased.asInstanceOf[Int]
        (key, restored)
      }
      .take(10)
      .foreach(println)
  }
}

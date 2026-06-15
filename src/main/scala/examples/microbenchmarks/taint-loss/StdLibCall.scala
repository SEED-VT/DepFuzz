package examples.microbenchmarks.taintloss

import org.apache.spark.{SparkConf, SparkContext}

/** Theoretical limitation: standard-library calls.
  *
  * Individual stdlib functions can be replaced with tainted versions (e.g. `MathSym.min`),
  * but no finite instrumentation strategy can cover the full space of library APIs.
  *
  * `math.min(taintedInt, 3)` resolves to plain `java.lang.Math.min`; taint is dropped at
  * the call boundary regardless of how many individual wrappers are added elsewhere.
  */
object StdLibCall {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    conf.setAppName("StdLibCall")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { cols => (cols(0).toInt, cols(1).toInt) }
      .map { s =>
        val capped = math.min(s._2, 3)
        (s._1, capped)
      }
      .take(10)
      .foreach(println)
  }
}

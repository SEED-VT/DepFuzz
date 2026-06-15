package examples.microbenchmarks.taintloss

import org.apache.spark.{SparkConf, SparkContext}

/** Theoretical limitation: pattern matching.
  *
  * Implicit conversions do not apply to type tests in `match` expressions. If the scrutinee
  * is tainted, `case x: Int => ...` is ill-typed unless taint is preserved explicitly in
  * the scrutinee type; the safest general rule is to avoid stripping taint from variables
  * that participate in case analysis.
  *
  * After instrumentation rewrites `Int` to `TaintedInt`, this program fails to compile.
  */
object PatternMatching {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    conf.setAppName("PatternMatching")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(args(0)).map(_.split(","))
      .map { cols => cols(1).toInt }
      .map { n =>
        n match {
          case x: Int if x > 100 => ("high", x)
          case x: Int            => ("low", x)
        }
      }
      .take(10)
      .foreach(println)
  }
}

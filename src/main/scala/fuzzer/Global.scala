package fuzzer

import scala.collection.mutable.Map

object Global {
  var iteration = 0
  var maxErrors = 0
  var stdout = ""
  var maxErrorsMap: Map[Vector[Any], (Throwable, Int, Int)] = Map[Vector[Any], (Throwable, Int, Int)]()
}

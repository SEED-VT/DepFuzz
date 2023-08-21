package fuzzer

import scala.collection.mutable._


class FuzzStats(val name: String) {
  var failures = 0
  val plotData: (ListBuffer[Int], ListBuffer[Double]) = (ListBuffer[Int](), ListBuffer[Double]())
  val inputs: Seq[String] = ListBuffer[String]()
  val failureMap: Map[Vector[Any], (Throwable, Int, Int)] = HashMap[Vector[Any], (Throwable, Int, Int)]()
  var cumulativeError = List[Int]()


  def add_plot_point(x: Int, y: Double): Unit = {
    if(plotData._2.length > 0 && y == plotData._2.last)
      return

    plotData._1.append(x)
    plotData._2.append(y)
  }
}

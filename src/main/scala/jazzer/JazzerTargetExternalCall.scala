package jazzer

import com.code_intelligence.jazzer.api.FuzzedDataProvider

object JazzerTargetExternalCall {

  var mode: String = ""
  var pkg: String = ""
  var measurementsDir: String = ""
  val datasets: Array[String] = Array(
    "/inputs/ds1"
  )

  def fuzzerInitialize(args: Array[String]): Unit = {
    measurementsDir = args(0)
    mode = args(1)
    pkg = args(2)

    SharedJazzerLogic.createMeasurementDir(measurementsDir)
  }

  def fuzzerTestOneInput(data: FuzzedDataProvider): Unit = {

    val f: Array[String] => Unit = pkg match {
      case "faulty" => examples.faulty.ExternalCall.main
//      case _ => examples.fuzzable.CommuteType.main
    }

    mode match {
      case "reproduce" => SharedJazzerLogic.fuzzTestOneInput(
        data,
        datasets,
        f
      )
      case "fuzz" => SharedJazzerLogic.fuzzTestOneInput(
        data,
        f,
        measurementsDir,
        datasets
      )
    }
  }

}

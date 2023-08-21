package jazzer

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import scoverage.Platform.FileWriter
import scoverage.Serializer
import utils.{FileUtils, IOUtils}

import java.io.File

object SharedJazzerLogic {

  var i = 0
  var prevCov = 0.0

  def updateIteration(measurementsDir: String): Unit = {
    i+=1
    new FileWriter(new File(s"$measurementsDir/iter"))
    .append(s"$i\n")
    .flush()
  }

  def fuzzTestOneInput(
                        data: FuzzedDataProvider,
                        f: Array[String] => Unit,
                        measurementsDir: String,
                        datasets: Array[String]): Unit = {

    val newDatasets = createMutatedDatasets(data, datasets)

    updateIteration(measurementsDir)
    
    var throwable: Throwable = null
    try { f(newDatasets) } 
    catch {
      case e: Throwable =>
        throwable = e
    }
    finally {
      SharedJazzerLogic.trackCumulativeCoverage(measurementsDir)
    }

    if (throwable != null)
      throw throwable
  }

  def fuzzTestOneInput(
                        data: FuzzedDataProvider,
                        datasets: Array[String],
                        f: Array[String] => Unit
                      ): Unit = {

    val newDatasets = createMutatedDatasets(data, datasets.map(s => s".$s"))
    f(newDatasets)
  }

  def trackCumulativeCoverage(measurementsDir: String): Unit = {
    val coverage = Serializer.deserialize(new File(s"$measurementsDir/scoverage.coverage")) // scoverage.coverage will be produced at compiler time by ScoverageInstrumenter.scala
    val measurementFiles = IOUtils.findMeasurementFiles(measurementsDir)
    val measurements = scoverage.IOUtils.invoked(measurementFiles)
    coverage.apply(measurements)
    if(coverage.statementCoveragePercent > prevCov) {
      new FileWriter(new File(s"$measurementsDir/cumulative.csv"), true)
          .append(s"$i,${coverage.statementCoveragePercent}")
          .append("\n")
          .flush()
      prevCov = coverage.statementCoveragePercent
    }
  }

  def createMeasurementDir(path: String): Unit = {
    new File(path).mkdirs()
  }


  def createMutatedDatasets(provider: FuzzedDataProvider, datasets: Array[String]): Array[String] = {
    datasets.map{ path => createMutatedDataset(provider, path) }
  }

  def createMutatedDataset(provider: FuzzedDataProvider, path: String): String = {
    val data = provider.consumeRemainingAsAsciiString().split("\n")
    FileUtils.writeToFile(data.toSeq, s"$path/part-00000")
    path
  }
}

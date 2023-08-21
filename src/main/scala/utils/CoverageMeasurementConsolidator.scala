package utils

import scoverage.Serializer
import scoverage.report.ScoverageHtmlWriter


import java.io.File

object CoverageMeasurementConsolidator {

  // Sample invocation: program MEASUREMENTS_DIR SRC_CODE_DIR REPORT_DIR
  //    MEASUREMENTS_DIR:   Where the .measurement files are stored for the execution e.g. target/jazzer-output/<programName>/measurements
  //    SRC_CODE_DIR: Source code needed for HTML output, not crucial but useful for debugging e.g src/main/scala
  //    REPORT_DIR: Where the generated report must be stored e.g. target/jazzer-output/<programName>/report
  def main(args: Array[String]): Unit = {

    val measurementsDir = args(0)
    val srcDir = args(1)
    val reportOutputDir = args(2)
    val coverage = Serializer.deserialize(new File(s"$measurementsDir/scoverage.coverage")) // scoverage.coverage will be produced at compiler time by ScoverageInstrumenter.scala
    val measurementFiles = IOUtils.findMeasurementFiles(measurementsDir)
    val measurements = scoverage.IOUtils.invoked(measurementFiles)

    coverage.apply(measurements)

    new ScoverageHtmlWriter(Seq(new File(srcDir)), new File(reportOutputDir)).write(coverage)
  }

}

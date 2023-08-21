package utils

import runners.Config
import scoverage.report.ScoverageHtmlWriter
import scoverage.{Coverage, ScoverageOptions, Serializer}

import java.io.File

object CompilerUtils {

  def CompileWithScoverage(path: String, coverageOutDir: String) : Unit = {
    val scoverageOptions = new ScoverageOptions()
    scoverageOptions.dataDir = coverageOutDir

    val compiler = ScoverageCompiler.default
    compiler.settings.outdir.value = s"target/scala-${Config.scalaVersion}/classes"
    compiler.settings.Xprint.value = List()
    compiler.settings.Yposdebug.value = false
    compiler.setOptions(scoverageOptions)

    compiler.compileSourceFiles(new File(path))
  }

  def ProcessCoverage(outputDir: String, createHTML: Boolean = true): (Coverage, Int) = {

    val coverage = Serializer.deserialize(new File(s"$outputDir/scoverage.coverage"))
    val measurementFiles = IOUtils.findMeasurementFiles(outputDir)
    val measurements = scoverage.IOUtils.invoked(measurementFiles)

    coverage.apply(measurements)

    if(createHTML)
      new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(outputDir)).write(coverage)

    (coverage, measurementFiles.length)
  }
}

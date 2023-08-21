package evaluators

import fuzzer.Program
import runners.Config
import scoverage.report.ScoverageHtmlWriter
import scoverage.{IOUtils, Serializer}
import utils.CompilerUtils.CompileWithScoverage

import java.io.File

object ComputeCoverage {

  def main(args: Array[String]): Unit = {

    val runs = 1

    // ==P.U.T. dependent configurations=======================
    val benchmark_name = Config.benchmarkName
    val Some(input_files) = Config.mapInputFilesFull.get(benchmark_name)
    val Some(fun_runnable) = Config.mapFunFuzzables.get(benchmark_name)
    val benchmark_class = Config.benchmarkClass
    // ========================================================

    val benchmark_path = s"src/main/scala/${benchmark_class.split('.').mkString("/")}.scala"
    val output_dir = "src/main/scala/run_with_coverage_output"

    val program = new Program(benchmark_name,
      benchmark_class,
      benchmark_path,
      fun_runnable,
      input_files)

    // Compile and instrument program with scoverage
    CompileWithScoverage(program.classpath, output_dir)
    // Run the monitored program, this will output coverage information to $output_dir
    program.main(program.args)

    val coverage = Serializer.deserialize(new File(s"$output_dir/scoverage.coverage"))
    val measurementFiles = IOUtils.findMeasurementFiles(output_dir)
    val measurements = IOUtils.invoked(measurementFiles)

    coverage.apply(measurements)
    new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(output_dir)).write(coverage)

    // Printing results
    println(s"Coverage: ${limitDP(coverage.statementCoveragePercent, 2)}% (gathered from ${measurementFiles.length} measurement file(s))")
    println(
      s"Config:\n" +
        s"\tProgram: ${program.name}\n" +
        s"\tIterations: $runs"
    )
  }

  def limitDP(d: Double, dp: Int): Double = {
    BigDecimal(d).setScale(dp, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}

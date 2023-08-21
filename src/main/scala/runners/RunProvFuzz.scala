package runners

import fuzzer._
import guidance.ProvFuzzGuidance
import scoverage.report.ScoverageHtmlWriter
import scoverage.{IOUtils, Serializer}
import utils.ProvFuzzUtils

import java.io.File

object RunProvFuzz {

  def main(args: Array[String]): Unit = {

    val runs = Config.fuzzDuration

    // ==P.U.T. dependent configurations=======================
    val benchmarkName = Config.benchmarkName
    val Some(inputFiles) = Config.mapInputFilesReduced.get(benchmarkName)
    val Some(inputFilesFull) = Config.mapInputFilesWeak.get(benchmarkName)
    val Some(funFuzzable) = Config.mapFunFuzzables.get(benchmarkName)
    val Some(schema) = Config.mapSchemas.get(benchmarkName)
    val benchmarkClass = Config.benchmarkClass
    val Some(funProbeAble) = Config.mapFunProbeAble.get(benchmarkName)
    // ========================================================

    val outputDir = s"${Config.resultsDir}/ProvFuzz"
    val scoverageResultsDir = s"$outputDir/scoverage-results"

    val benchmarkPath = s"src/main/scala/${benchmarkClass.split('.').mkString("/")}.scala"
    val program = new Program(
      benchmarkName,
      benchmarkClass,
      benchmarkPath,
      funFuzzable,
      inputFiles)


    val probeClass = s"examples.monitored.$benchmarkName"
    val probePath = s"src/main/scala/${probeClass.split('.').mkString("/")}.scala"
    val probeProgram = new InstrumentedProgram(benchmarkName,
      probeClass,
      probePath,
      funProbeAble,
      inputFilesFull)

    // Probing and Fuzzing
//    val probingDataset = ProvFuzzUtils.CreateProbingDatasets(probeProgram, schema)
    val (provInfo, timeStartProbe, timeEndProbe) = ProvFuzzUtils.Probe(probeProgram)
    val guidance = new ProvFuzzGuidance(inputFiles, provInfo, runs)
    val (stats, timeStartFuzz, timeEndFuzz) = NewFuzzer.FuzzMutants(program, program, guidance, outputDir)

    // Finalizing
//    val coverage = Serializer.deserialize(new File(s"$scoverageResultsDir/scoverage.coverage"))
//    val measurementFiles = IOUtils.findMeasurementFiles(scoverageResultsDir)
//    val measurements = IOUtils.invoked(measurementFiles)

//    coverage.apply(measurements)
//    new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(scoverageResultsDir)).write(coverage)

    val durationProbe = (timeEndProbe - timeStartProbe) / 1000.0
    val durationFuzz = (timeEndFuzz - timeStartFuzz) / 1000.0
    val durationTotal = durationProbe + durationFuzz


    // Printing results
    stats.failureMap.foreach { case (msg, (_, c, i)) => println(s"i=$i:line=${getLineNo(benchmarkName, msg.mkString(","))} $c x $msg") }
    stats.failureMap.foreach { case (msg, (_, c, i)) => println(s"i=$i:line=${getLineNo(benchmarkName, msg.mkString(","))} x $c") }
    stats.failureMap.map { case (msg, (_, c, i)) => (getLineNo(benchmarkName, msg.mkString("\n")), c, i) }
      .groupBy(_._1)
      .map { case (line, list) => (line, list.size) }
      .toList.sortBy(_._1)
      .foreach(println)

    println(s"=== RESULTS: ProvFuzz $benchmarkName ===")
    println(s"Failures: ${stats.failures} (${stats.failureMap.keySet.size} unique)")
    println(s"failures: ${stats.failureMap.map { case (_, (_, _, i)) => i + 1 }.toSeq.sortBy(i => i).mkString(",")}")
    println(s"coverage progress: ${stats.plotData._2.map(limitDP(_, 2)).mkString(",")}")
    println(s"iterations: ${stats.plotData._1.mkString(",")}")
//    println(s"Coverage: ${limitDP(coverage.statementCoveragePercent, 2)}% (gathered from ${measurementFiles.length} measurement files)")
    println(s"Total Time (s): ${limitDP(durationTotal, 2)} (P: $durationProbe | F: $durationFuzz)")
    println(
      s"Config:\n" +
        s"\tProgram: ${program.name}\n" +
        s"\tMutation Distribution M1-M6: ${guidance.mutate_probs.mkString(",")}\n" +
        s"\tActual Application: ${guidance.actual_app.mkString(",")}\n" +
        s"\tIterations: ${Global.iteration}"
    )
    println("ProvInfo: ")
    println(provInfo)
  }

  def limitDP(d: Double, dp: Int): Double = {
    BigDecimal(d).setScale(dp, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def getLineNo(filename: String, trace: String): String = {
    val pattern = s"""${filename}.scala:(\\d+)"""
    pattern.r.findFirstIn(trace) match {
      case Some(str) => str.split(':').last
      case _ => "-"
    }
  }
}

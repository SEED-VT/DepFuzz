package runners

import fuzzer.InstrumentedProgram
import utils.ProvFuzzUtils


object RunProvInfoExtractor {

  def main(args: Array[String]): Unit = {

    println("RunFuzzerJar called with following args:")
    println(args.mkString("\n"))

    // ==P.U.T. dependent configurations=======================
    val benchmarkName = args(0)
    val Some(inputFiles) = Config.mapInputFilesReduced.get(benchmarkName)
    val Some(funProbeAble) = Config.mapFunProbeAble.get(benchmarkName)
    // ========================================================

    val probeClass = s"examples.monitored.$benchmarkName"
    val probePath = s"src/main/scala/${probeClass.split('.').mkString("/")}.scala"
    val probeProgram = new InstrumentedProgram(benchmarkName,
      probeClass,
      probePath,
      funProbeAble,
      inputFiles)

    val (provInfo, _, _) = ProvFuzzUtils.Probe(probeProgram)
    println("ProvInfo: ")
    println(provInfo)

    sys.exit(0)
//    val (stats, timeStartFuzz, timeEndFuzz) = Fuzzer.Fuzz(program, guidance, outDir)

    // Finalizing
//    val coverage = Serializer.deserialize(new File(s"$scoverageOutputDir/scoverage.coverage"))
//    val measurementFiles = IOUtils.findMeasurementFiles(scoverageOutputDir)
//    val measurements = IOUtils.invoked(measurementFiles)

//    coverage.apply(measurements)
//    new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(scoverageOutputDir)).write(coverage)

//    val durationProbe = 0.1f // (timeEndProbe - timeStartProbe) / 1000.0
//    val durationFuzz = (timeEndFuzz - timeStartFuzz) / 1000.0
//    val durationTotal = durationProbe + durationFuzz


    // Printing results
//    stats.failureMap.foreach { case (msg, (_, c, i)) => println(s"i=$i:line=${getLineNo(benchmarkName, msg.mkString(","))} $c x $msg") }
//    stats.failureMap.foreach { case (msg, (_, c, i)) => println(s"i=$i:line=${getLineNo(benchmarkName, msg.mkString(","))} x $c") }
//    stats.failureMap.map { case (msg, (_, c, i)) => (getLineNo(benchmarkName, msg.mkString("\n")), c, i) }
//      .groupBy(_._1)
//      .map { case (line, list) => (line, list.size) }
//      .toList.sortBy(_._1)
//      .foreach(println)

//    println(s"=== RESULTS: ProvFuzz $benchmarkName ===")
//    println(s"Failures: ${stats.failures} (${stats.failureMap.keySet.size} unique)")
//    println(s"failures: ${stats.failureMap.map { case (_, (_, _, i)) => i + 1 }.toSeq.sortBy(i => i).mkString(",")}")
//    println(s"coverage progress: ${stats.plotData._2.map(limitDP(_, 2)).mkString(",")}")
//    println(s"iterations: ${stats.plotData._1.mkString(",")}")
//    println(s"Coverage: ${limitDP(coverage.statementCoveragePercent, 2)}% (gathered from ${measurementFiles.length} measurement files)")
//    println(s"Total Time (s): ${limitDP(durationTotal, 2)} (P: $durationProbe | F: $durationFuzz)")
//    println(
//      s"Config:\n" +
//        s"\tProgram: ${program.name}\n" +
//        s"\tMutation Distribution M1-M6: ${guidance.get_mutate_probs.mkString(",")}\n" +
//        s"\tActual Application: ${guidance.get_actual_app.mkString(",")}\n" +
//        s"\tIterations: ${Global.iteration}"
//    )
//    println("ProvInfo: ")
//    println(provInfo)
  }

  def limitDP(d: Double, dp: Int): Double = {
    BigDecimal(d).setScale(dp, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def getLineNo(filename: String, trace: String): String = {
    val pattern = s"""$filename.scala:(\\d+)"""
    pattern.r.findFirstIn(trace) match {
      case Some(str) => str.split(':').last
      case _ => "-"
    }
  }
}

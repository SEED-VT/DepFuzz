package fuzzer

import runners.Config
import scoverage.Platform.FileWriter
import scoverage.report.ScoverageHtmlWriter
import scoverage.{Constants, Coverage, IOUtils, Serializer}
import utils.CompilerUtils.CompileWithScoverage
import utils.FileUtils

import java.io.{File, FileFilter}
import scala.reflect.io.Directory

object NewFuzzer {

  def getMeasurementFile(dataDir: File, iteration: Int): Array[File] = dataDir.listFiles(new FileFilter {
    override def accept(pathname: File): Boolean = pathname.getName.startsWith(s"${Constants.MeasurementsPrefix}")
  })

  def getCoverage(dataDir: String, iteration: Int = 0): Coverage = {
    val coverage = Serializer.deserialize(new File(s"$dataDir/${Constants.CoverageFileName}"))
    val measurementFiles = getMeasurementFile(new File(dataDir), iteration)
    coverage.apply(IOUtils.invoked(measurementFiles))
    coverage
  }

  def writeToFile(outDir: String, data: Seq[String], dataset: Int) : String = {
    val output_dir = s"$outDir/dataset_$dataset"
    val output_file = s"$output_dir/part-000000"
    FileUtils.writeToFile(data, output_file)
    output_dir
  }

  def writeErrorToFile(error: Vector[String], outDir: String): Unit = {
    val writer = new FileWriter(new File(outDir), true)
    writer
      .append(s"${Global.iteration},${error.mkString(" : ")}")
      .append("\n")
      .flush()

    writer.close()
  }

  def writeStringToFile(outDir: String, str: String): Unit = {
    val writer = new FileWriter(new File(outDir))
    writer
      .append(str)
      .append("\n")
      .flush()

    writer.close()
  }

  def getElapsedSeconds(t_start: Long): Float = {
    (System.currentTimeMillis() - t_start)/1000.0f + 1.0f
  }

  def FuzzMutants(refProgram: ExecutableProgram, mutantProgram: ExecutableProgram, guidance: Guidance, outDir: String, compile: Boolean = true): (FuzzStats, Long, Long) = {
    val testCaseOutDir = s"$outDir/interesting-inputs"
    val coverageOutDir = s"$outDir/scoverage-results"
    val refCoverageOutDir = s"$coverageOutDir/referenceProgram"
    val mutantCoverageOutDir = s"$coverageOutDir/${mutantProgram.name}"
    var stats = new FuzzStats(refProgram.name)
    var mutantStats = new FuzzStats(mutantProgram.name)
    var lastCoverage = 0.0;
    var mutantLastCoverage = 0.0;

    if(compile) {
      new Directory(new File(outDir)).deleteRecursively()
      CompileWithScoverage(refProgram.classpath, refCoverageOutDir)
//      CompileWithScoverage(mutantProgram.classpath, mutantCoverageOutDir)
    }

    val t_start = System.currentTimeMillis()
    var mutantKilled = false
    var postMutantKill = false
    var mutantKilledThisIter = false
    var refExecStats: ExecStats = null
    var mutantExecStats: ExecStats = null
    while(!guidance.isDone()) {
      var outDirTestCase = s"$testCaseOutDir/iter_${fuzzer.Global.iteration}"
      val inputDatasets = guidance.getInput().map(f => FileUtils.readDatasetPart(f, 0))
      val mutated_files = guidance.mutate(inputDatasets).zipWithIndex.map{case (e, i) => writeToFile(outDirTestCase, e, i)}
      if(!mutantKilled) { // maybe should remove this, is there a point in continuing after mutant is killed?
        val (same, _refExecStats, _mutantExecStats) = compareExecutions(refProgram, mutantProgram, mutated_files)
        refExecStats = _refExecStats
        mutantExecStats = _mutantExecStats
        mutantKilled = !same
        if (mutantKilled) {
          // handle divering output
          // probably should stop fuzzing here
          val newOutDirTestCase = s"${outDirTestCase}_diverging"
          new File(outDirTestCase).renameTo(new File(newOutDirTestCase))
          outDirTestCase = newOutDirTestCase
          mutantKilledThisIter = true
//          return (stats, t_start, System.currentTimeMillis())
        }
      } else {
        val execInfo = exec(refProgram, mutated_files)
        postMutantKill = true
      }

      val (newStats, newLastCoverage, changed) = analyzeAndLogCoverage(refCoverageOutDir, stats, lastCoverage, t_start)
//      val (newMutantStats, newMutantLastCoverage, _) = analyzeAndLogCoverage(mutantCoverageOutDir, mutantStats, mutantLastCoverage)

      logTimeAndIteration(outDir, t_start)
      guidance.updateCoverage(getCoverage(refCoverageOutDir, fuzzer.Global.iteration))

      if(changed || mutantKilledThisIter) {
        logTimeAndIteration(outDirTestCase,t_start)
        writeStringToFile(s"$outDirTestCase/ref_output.stdout", refExecStats.stdout)
        writeStringToFile(s"$outDirTestCase/ref_output.stderr", refExecStats.stderr)
        if(!postMutantKill) {
          writeStringToFile(s"$outDirTestCase/mutant_output.stdout", mutantExecStats.stdout)
          writeStringToFile(s"$outDirTestCase/mutant_output.stderr", mutantExecStats.stderr)
        }
        new File(outDirTestCase).renameTo(new File(s"${outDirTestCase}_newCov_${newLastCoverage}"))
      }

      if(!changed && (!mutantKilled || postMutantKill)) {
        new Directory(new File(outDirTestCase)).deleteRecursively()
      }

      stats = newStats
//      mutantStats = newMutantStats
      lastCoverage = newLastCoverage
//      mutantLastCoverage = newMutantLastCoverage
      fuzzer.Global.iteration += 1
      mutantKilledThisIter = false
    }

    new FileWriter(new File(s"$refCoverageOutDir/coverage.tuples"), true)
      .append(s"(${getElapsedSeconds(t_start)},${lastCoverage}) % iter=${Global.iteration} ")
      .append("\n")
      .flush()

    val coverage = getCoverage(refCoverageOutDir)
    new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(refCoverageOutDir)).write(coverage)

    (stats, t_start, System.currentTimeMillis())
  }

  def constructErrorReport(e: Throwable) = {
    s"$e\nmessage: ${e.getMessage}\nstack_trace:-\n${e.getStackTrace.mkString("\n")}"
  }

  def exec(program: ExecutableProgram, args: Array[String]): ExecStats = {
    val execStats = try {
      program.invokeMain(args)
      new ExecStats(Global.stdout, "", args, false)
    } catch {
      case e: Throwable => new ExecStats(Global.stdout, constructErrorReport(e), args, true)
    }
    Global.stdout = ""
    execStats
  }

  def isErrorSame(o1: ExecStats, o2: ExecStats): Boolean = {
    true
  }

  def compareOutputs(o1: ExecStats, o2: ExecStats): Boolean = {
    val sameTerminationStatus = !(o1.crashed ^ o2.crashed)
    val sameOutput = if(sameTerminationStatus && !o1.crashed) o1.stdout == o2.stdout else true
    val sameError = if(sameTerminationStatus && o1.crashed) isErrorSame(o1, o2) else true // TODO: this will be tricky i think, skip for now

    val same = sameTerminationStatus && sameOutput && sameError

//    println(
//      s"""============COMPARE result: ${same} inputs: ${o1.input.mkString("(",",",")")} iter: ${Global.iteration} ================
//         |------------REF crashed: ${o1.crashed} ---------------------
//         |${if(o1.crashed) o1.stderr else o1.stdout}
//         |------------MUTANT crashed: ${o2.crashed} ---------------------
//         |${if(o2.crashed) o2.stderr else o2.stdout}
//         |============COMPARE END================
//         |""".stripMargin)

    same
  }

  def compareExecutions(refProgram: ExecutableProgram, mutantProgram: ExecutableProgram, mutated_datasets: Array[String]): (Boolean, ExecStats, ExecStats) = {
    val execStatsRef = exec(refProgram, mutated_datasets)
    val execStatsMutant = exec(mutantProgram, mutated_datasets)
    (compareOutputs(execStatsRef, execStatsMutant), execStatsRef, execStatsMutant)
  }

  def updateCoverage(cov: Coverage, lastCoverage: Double): Boolean = {
    var changed = false
    if (Global.iteration == 0 || cov.statementCoveragePercent > lastCoverage) {
      changed = true;
    }
    changed
  }

  def analyzeAndLogCoverage(
                             refCoverageOutDir: String,
                             stats: FuzzStats,
                             lastCoverage: Double,
                             t_start: Long): (FuzzStats, Double, Boolean) = {

    var newCov = lastCoverage
    val coverage = getCoverage(refCoverageOutDir, fuzzer.Global.iteration)
    stats.add_plot_point(fuzzer.Global.iteration, coverage.statementCoveragePercent)
    val changed = updateCoverage(coverage, lastCoverage)
    if (changed) {
      newCov = coverage.statementCoveragePercent
      new FileWriter(new File(s"$refCoverageOutDir/coverage.tuples"), true)
        .append(s"(${getElapsedSeconds(t_start)},${coverage.statementCoveragePercent}) % iter=${Global.iteration} ")
        .append("\n")
        .flush()
    }

    // write coverage information to coverageOutDir
//    new ScoverageHtmlWriter(Seq(new File("src/main/scala")), new File(refCoverageOutDir)).write(coverage)
    (stats, newCov, changed)
  }

  def logTimeAndIteration(outDir: String, t_start: Long): Unit = {
    val writer = new FileWriter(new File(s"$outDir/iter_time"))
    writer
      .append(s"${Global.iteration},${(System.currentTimeMillis() - t_start)/1000.0f}")
      .append("\n")
      .flush()
    writer.close()
  }

}

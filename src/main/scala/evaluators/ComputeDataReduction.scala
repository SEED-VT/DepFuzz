package evaluators

import fuzzer.{InstrumentedProgram, Program}
import runners.Config
import scoverage.report.ScoverageHtmlWriter
import scoverage.{IOUtils, Serializer}
import utils.CompilerUtils.{CompileWithScoverage, ProcessCoverage}
import utils.ProvFuzzUtils

import java.io.File

object ComputeDataReduction {
//
//  def main(args: Array[String]): Unit = {
//
//    // ==P.U.T. dependent configurations=======================
//    val benchmarkName = Config.benchmarkName
//    val Some(inputFiles) = Config.mapInputFilesFull.get(benchmarkName)
//    val Some(fun_runnable) = Config.mapFunFuzzables.get(benchmarkName)
//    val benchmarkClass = Config.benchmarkClass
//    val Some(fun_probe_able) = Config.mapFunProbeAble.get(benchmarkName)
//    // ========================================================
//
//    val benchmark_path = s"src/main/scala/${benchmarkClass.split('.').mkString("/")}.scala"
//    val results_dir = "target/evaluation-results/reduction"
//    val data_reduced = s"$results_dir/data_reduced"
//    val data_sampled = s"$results_dir/data_sampled"
//    val full_coverage_dir = s"$results_dir/coverage_full"
//    val reduced_coverage_dir = s"$results_dir/coverage_reduced"
//    val sampled_coverage_dir = s"$results_dir/coverage_sampled"
//    val probe_class = s"examples.monitored.probe_$benchmarkName"
//    val probe_path = s"src/main/scala/${probe_class.split('.').mkString("/")}.scala"
//
//    val program = new Program(
//      benchmarkName,
//      benchmarkClass,
//      benchmark_path,
//      fun_runnable,
//      inputFiles)
//
//    val probe_program = new InstrumentedProgram(
//      benchmarkName,
//      probe_class,
//      probe_path,
//      fun_probe_able,
//      inputFiles)
//
//    //Get minimum input set
//    val probe_info = probe_program.main(probe_program.args)
//    val red_data_len = probe_info.min_data.map(_.length).foldLeft(0)(_+_)
//    val full_data_len = generators.GenSegmentationData.dataper*probe_info.min_data.length
//    val red_data_per = red_data_len.toDouble/full_data_len.toDouble*100
//
////    Check coverage of program on full and reduced input
////    CompileWithScoverage(program.classpath, full_coverage_dir)
////    program.main(program.args)
//    val (full_coverage, _) = ProcessCoverage(full_coverage_dir, false)
//
////    val reduced_files = ProvFuzzUtils.WriteDatasets(probe_info.min_data, data_reduced)
////    CompileWithScoverage(program.classpath, reduced_coverage_dir)
////    program.main(reduced_files)
//    val (reduced_coverage, _) = ProcessCoverage(reduced_coverage_dir, false)
//
//    val sampled_files = ProvFuzzUtils.RandomSampleDatasets(inputFiles, data_sampled, red_data_per)
//    CompileWithScoverage(program.classpath, sampled_coverage_dir)
//    program.main(sampled_files)
//  val (sampled_coverage, _) = ProcessCoverage(sampled_coverage_dir, false)
//
//    probe_info.min_data.zipWithIndex.foreach{
//      case (dataset, i) =>
//        println(s"Dataset $i")
//        dataset.foreach(println)
//        println("--------------------\n")
//    }
//    println(s"Data Reduced To: ${red_data_per}% of original | $red_data_len/$full_data_len")
//    println(s"Coverage: ${limitDP(full_coverage.statementCoveragePercent, 2)}% (full data)")
//    println(s"Coverage: ${limitDP(reduced_coverage.statementCoveragePercent, 2)}% (reduced data)")
//    println(s"Coverage: ${limitDP(sampled_coverage.statementCoveragePercent, 2)}% (sampled data)")
//  }
//
//  def limitDP(d: Double, dp: Int): Double = {
//    BigDecimal(d).setScale(dp, BigDecimal.RoundingMode.HALF_UP).toDouble
//  }
}
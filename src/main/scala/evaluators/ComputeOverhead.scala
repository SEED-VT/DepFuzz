package evaluators

import fuzzer.{InstrumentedProgram, Program}
import runners.Config

object ComputeOverhead {

  def runAndTimeProgram[T](program: Array[String] => T, input_files: Array[String]): (Long, Long) = {
    val t_start = System.currentTimeMillis()
    program(input_files)
    (t_start, System.currentTimeMillis())
  }

  def main(args: Array[String]): Unit = {

    // ==P.U.T. dependent configurations=======================
    val benchmark_name = Config.benchmarkName
    val Some(input_files) = Config.mapInputFilesFull.get(benchmark_name)
    val Some(fun_spark) = Config.mapFunSpark.get(benchmark_name)
    val benchmark_class = Config.benchmarkClass
    val Some(fun_probe_able) = Config.mapFunProbeAble.get(benchmark_name)
    // ========================================================

    val benchmark_path = s"src/main/scala/${benchmark_class.split('.').mkString("/")}.scala"
    val spark_program = new Program(
      benchmark_name,
      benchmark_class,
      benchmark_path,
      fun_spark,
      input_files)

    val (ts_spark, te_spark) = runAndTimeProgram(spark_program.main, input_files)
    println(s"${te_spark - ts_spark} ms : Original Program")


//    val probe_class = s"examples.monitored.probe_$benchmarkName"
//    val probe_path = s"src/main/scala/${probe_class.split('.').mkString("/")}.scala"
//    val probe_program = new InstrumentedProgram(
//      benchmarkName,
//      probe_class,
//      probe_path,
//      fun_probe_able,
//      inputFiles)
//    val (ts_instr, te_instr) = runAndTimeProgram(probe_program.main, inputFiles)
//    println(s"${te_instr - ts_instr} ms : Instrumented Program")

//    Program ,  (ms), Instrumented (ms)
//    Customers
//      Original: 4019,
//      Instrumt: 8178, 7711, 9258, 8385, 8229
  }
}

package runners

import examples.benchmarks
import fuzzer.{ProvInfo, Schema}
import schemas.BenchmarkSchemas

import scala.collection.mutable.ListBuffer

object Config {

  val scalaVersion = 2.12
  val maxSamples = 5
  val maxRepeats = 1
  val percentageProv = 0.1f
  val iterations = 10
  val fuzzDuration = 10 // 86400 // duration in seconds
  val benchmarkName = "WebpageSegmentation"
  val resultsDir = s"./target/fuzzer-results/$benchmarkName"
  val faultTest = true
  val deepFaults = false
  val seedType = "weak" //either full, reduced or weak
  val benchmarkClass = s"examples.${if (faultTest) "faulty" else "fuzzable"}.$benchmarkName"
  val mutateProbs: Array[Float] = Array( // 0:M1, 1:M2 ... 5:M6
    0.9f, // Data
    0.02f, // Data
    0.02f, // Format
    0.02f, // Format
    0.02f, // Format
    0.02f) // Format


  val mutateProbsProvFuzz: Array[Float] = Array( // 0:M1, 1:M2 ... 5:M6
    0.9f, // Data
    0.000000000000000000f, // Data
    0.000000000000000000f, // Format
    0.000000000000000000f, // Format
    0.000000000000000000f, // Format
    0.000000000000000000f) // Format

}

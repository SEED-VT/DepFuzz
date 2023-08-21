package fuzzer

import scoverage.Coverage

trait Guidance {

  def get_actual_app: Array[Int] = ???
  def get_mutate_probs: Array[Float] = ???

  def mutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]]

  def updateCoverage(coverage: Coverage, outDir: String = "/dev/null", crashed: Boolean = true): Boolean

  def getInput(): Array[String]

  def isDone(): Boolean
}

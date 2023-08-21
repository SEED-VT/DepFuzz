package guidance

import fuzzer.{Guidance, Schema}
import scoverage.Coverage
import utils.FileUtils
import utils.MutationUtils._


class NoGuidance(val input_files: Array[String], val schemas: Array[Array[Schema[Any]]], val max_runs: Int) extends Guidance {
  var last_input = input_files
  var coverage: Coverage = new Coverage
  var runs = 0


  def mutateRow(row: String): String = {
    row.map(mutateChar(_, 0.3f))
  }


  def mutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]] = {
    inputDatasets
  }

  override def getInput(): Array[String] = {
    this.last_input
  }

  override def isDone(): Boolean = {
    runs += 1
    runs > this.max_runs
  }

  override def updateCoverage(_coverage: Coverage, outDir: String = "/dev/null", crashed: Boolean = true): Boolean = {
    this.coverage = _coverage
    true
  }
}

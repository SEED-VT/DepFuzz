package guidance

import fuzzer.{Global, Guidance, Schema}
import scoverage.Coverage
import scoverage.Platform.FileWriter
import utils.MutationUtils._

import java.io.File
import scala.concurrent.duration._


class RandomGuidance(val input_files: Array[String], val schemas: Array[Array[Schema[Any]]], val duration: Int) extends Guidance {
  var last_input = input_files
  var coverage: Coverage = new Coverage
  val deadline = duration.seconds.fromNow
  var runs = 0
  val max_row_dups = 10
  val max_col_dups = 10
  val row_dup_prob = 0.5f
  val col_dup_prob = 0f
  val skip_prob = 0.1f


  def mutateRow(row: String): String = {
    row.map(mutateChar(_, 0.3f))
  }

  // Mutates a single dataset (Each dataset is mutated independently in BigFuzz)
  def mutate(input: Seq[String], dataset: Int): Seq[String] = {
    randomDuplications(input, this.max_row_dups, this.row_dup_prob)
      .map(row => mutateRow(row))
//      .map(row => mutateRow(randomDuplications(row.split(','), this.max_col_dups, this.col_dup_prob).mkString(",")))
  }

  // Mutates all datasets
  def mutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]] = {
    val mutated_datasets = inputDatasets.zipWithIndex.map{case (d, i) => mutate(d, i)}
    mutated_datasets
//    this.last_input = mutated_datasets.zipWithIndex.map{case (e, i) => writeToFile(e, i)}
//    this.last_input
  }

  override def getInput(): Array[String] = {
    this.last_input
  }

  override def isDone(): Boolean = {
    !deadline.hasTimeLeft()
  }

  override def updateCoverage(cov: Coverage, outDir: String = "/dev/null", crashed: Boolean = true): Boolean = {
    if(Global.iteration == 0 || cov.statementCoveragePercent > this.coverage.statementCoveragePercent) {
      this.coverage = cov
      new FileWriter(new File(s"$outDir/cumulative.csv"), true)
        .append(s"${Global.iteration},${coverage.statementCoveragePercent}")
        .append("\n")
        .flush()
    }
    true
  }
}

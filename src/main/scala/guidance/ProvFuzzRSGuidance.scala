package guidance

import fuzzer.{Global, Guidance, Schema}
import runners.Config
import scoverage.Coverage
import scoverage.Platform.FileWriter
import utils.MutationUtils
import utils.MutationUtils._

import java.io.File
import scala.concurrent.duration._
import scala.util.Random


class ProvFuzzRSGuidance(val input_files: Array[String], val schemas: Array[Array[Schema[Any]]], val duration: Int) extends Guidance {
  var last_input = input_files
  val deadline = duration.seconds.fromNow
  var coverage: Coverage = new Coverage
  var runs = 0
  val mutate_probs: Array[Float] = Array( // 0:M1, 1:M2 ... 5:M6
    0.999f, // Data
    0.0002f, // Data
    0.0002f, // Format
    0.0002f, // Format
    0.0002f, // Format
    0.0002f) // Format

  val actual_app = Array.fill(mutate_probs.length){0}
  override def get_actual_app: Array[Int] = actual_app

  override def get_mutate_probs: Array[Float] = mutate_probs

  var app_total = 0

  def getSchema(d: Int, c: Int): Schema[Any]  = {
    try {
      this.schemas(d)(c)
    } catch {
      case _ => new Schema[Any](Schema.TYPE_OTHER)
    }
  }

  val mutations = Array[(String, Int, Int) => String] (
    M1,
    M2,
    M3,
    M4,
    M5,
    M6
  )
  val byte_mut_prob = 0.5f
  val max_col_drops = 2
  val max_row_dups = 10
  val max_col_dups = 10
  val row_dup_prob = 0.5f
  val col_dup_prob = 0.00000001f
  val skip_prob = 0.1f
  val oor_prob = 1.0f //out-of-range probability: prob that a number will be mutated out of range vs normal mutation

  def M1(e: String, c: Int, d: Int): String = {
    val schema = getSchema(d, c)
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob)// schema.values(Random.nextInt(schema.values.length)).toString
      case _ if schema.range == null => mutateNumber(e)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob)
    }
  }

  def M2(e: String, c: Int, d: Int): String = {
    val schema = getSchema(d, c)
    schema.dataType match {
      case Schema.TYPE_NUMERICAL => changeNumberFormat(e)
      case _ => M1(e, c, d)
    }
  }
  def M3(row: String, c: Int = -1, d: Int = -1): String = {
    val cols = row.split(',')
    if(cols.length < 2) {
      return row
    }
    val i = Random.nextInt(cols.length-1)
    cols.slice(0, i+1).mkString(",") + "~" + cols.slice(i+1, cols.length).mkString(",")
  }
  def M4(e: String, c: Int, d: Int): String = {
    M1(e, c, d)
  }

  // input: a dataset row
  // returns new row with random column(s) dropped
  def M5(e: String, c: Int, d: Int): String = {
    val cols = e.split(',').to
    val to_drop = (0 to Random.nextInt(this.max_col_drops)).map(_ => Random.nextInt(cols.length))
    cols.zipWithIndex.filter{ case (_, i) => !to_drop.contains(i)}.map(_._1).mkString(",")
  }

  // input: a column value
  // returns an empty column (BigFuzz Paper)
  def M6(e: String, c: Int, d: Int): String = {
    ""
  }

  def mutateCol(v: String, c: Int, d: Int): String = {
    val mutation_ids = Array(1, 2, 4, 5, 6).map(_-1)
    val probs = mutation_ids.map(this.mutate_probs(_))
    val to_apply = MutationUtils.RouletteSelect(mutation_ids, probs)
    this.actual_app(to_apply) += 1
    this.app_total += 1
    probabalisticApply(this.mutations(to_apply), v, c, d)
  }

  def mutateRow(row: String, dataset: Int): String = {
    probabalisticApply(M3, row.split(',').zipWithIndex.map{case (e, i) => mutateCol(e, i, dataset)}.mkString(","), prob=this.mutate_probs(2))
  }

  // Mutates a single dataset (Each dataset is mutated independently in BigFuzz)
  def mutate(input: Seq[String], dataset: Int): Seq[String] = {
    randomDuplications(input, this.max_row_dups, this.row_dup_prob)
      .map(row => mutateRow(randomDuplications(row.split(','), this.max_col_dups, this.col_dup_prob).mkString(","), dataset))
  }

  // Mutates all datasets
  def mutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]] = {
//    return inputFiles
    val mutatedDatasets = inputDatasets.zipWithIndex.map{case (d, i) => mutate(d, i)}
    mutatedDatasets
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

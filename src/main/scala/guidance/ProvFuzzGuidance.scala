package guidance

import fuzzer.{CoDepTuple, Global, Guidance, Operator, ProvInfo, Schema}
import runners.Config
import scoverage.Coverage
import scoverage.Platform.FileWriter
import utils.MutationUtils._
import utils.{FileUtils, MutationUtils}

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Random


class ProvFuzzGuidance(val inputFiles: Array[String], val provInfo: ProvInfo, val duration: Int) extends Guidance {
  var last_input = inputFiles
  var coverage: Coverage = new Coverage
  var runs = 0
  val deadline = duration.seconds.fromNow

  //Note to self: Each element here is the probability that a mutation M_n will be applied after it has already been selected
  //              This is NOT the probability of the mutation being selected
  val mutate_probs = Config.mutateProbsProvFuzz

  val actual_app = Array.fill(mutate_probs.length){0}
  var app_total = 0

  override def get_actual_app: Array[Int] = actual_app

  override def get_mutate_probs: Array[Float] = mutate_probs

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
  val col_dup_prob = 0f
  val skip_prob = 0.1f
  val oor_prob = 1.0f //out-of-range probability: prob that a number will be mutated out of range vs normal mutation


  def inferType(v: String): Schema[Any] = {
    try {
      v.toDouble
      new Schema[Any](Schema.TYPE_NUMERICAL)
    } catch {
      case _: NumberFormatException => new Schema[Any](Schema.TYPE_OTHER)
    }

  }

  def BFM1(e: String, c: Int, d: Int): String = {
    val schema = inferType(e)
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob)// schema.values(Random.nextInt(schema.values.length)).toString
      case _ if schema.range == null => mutateNumber(e)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob)
    }
  }

  def BFM2(e: String, c: Int, d: Int): String = {
    val schema = inferType(e)
    schema.dataType match {
      case Schema.TYPE_NUMERICAL => changeNumberFormat(e)
      case _ => e
    }
  }
  def BFM3(row: String, c: Int = -1, d: Int = -1): String = {
    val cols = row.split(',')
    if(cols.length < 2) {
      return row
    }
    val i = Random.nextInt(cols.length-1)
    cols.slice(0, i+1).mkString(",") + "~" + cols.slice(i+1, cols.length).mkString(",")
  }
  def BFM4(e: String, c: Int, d: Int): String = {
    M1(e, c, d)
  }

  // input: a dataset row
  // returns new row with random column(s) dropped
  def BFM5(e: String, c: Int, d: Int): String = {
    val cols = e.split(',').to
    val to_drop = (0 to Random.nextInt(this.max_col_drops)).map(_ => Random.nextInt(cols.length))
    cols.zipWithIndex.filter{ case (_, i) => !to_drop.contains(i)}.map(_._1).mkString(",")
  }

  // input: a column value
  // returns an empty column (BigFuzz Paper)
  def BFM6(e: String, c: Int, d: Int): String = {
    ""
  }

  def BFmutateCol(v: String, c: Int, d: Int): String = {
    val mutation_ids = Array(1, 2, 4, 5, 6).map(_-1)
    val probs = mutation_ids.map(this.mutate_probs(_))
    val to_apply = MutationUtils.RouletteSelect(mutation_ids, probs)
    this.actual_app(to_apply) += 1
    this.app_total += 1
    probabalisticApply(this.mutations(to_apply), v, c, d)
  }

  def BFmutateRow(row: String, dataset: Int): String = {
    probabalisticApply(M3, row.split(',').zipWithIndex.map{case (e, i) => BFmutateCol(e, i, dataset)}.mkString(","), prob=this.mutate_probs(2))
  }

  // Mutates a single dataset (Each dataset is mutated independently in BigFuzz)
  def BFmutate(input: Seq[String], dataset: Int): Seq[String] = {
    randomDuplications(input, this.max_row_dups, this.row_dup_prob)
      .map(row => BFmutateRow(randomDuplications(row.split(','), this.max_col_dups, this.col_dup_prob).mkString(","), dataset))
  }

  // Mutates all datasets
  def BFmutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]] = {
    //    return inputFiles
    val mutatedDatasets = inputDatasets.zipWithIndex.map{case (d, i) => BFmutate(d, i)}
    mutatedDatasets
    //    this.last_input = mutated_datasets.zipWithIndex.map{case (e, i) => writeToFile(e, i)}
    //    this.last_input
  }


  def M1(e: String, c: Int, d: Int): String = {
    val schema = inferType(e)
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob)// schema.values(Random.nextInt(schema.values.length)).toString
      case _ if schema.range == null => mutateNumber(e)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob)
    }
  }

  def M2(e: String, c: Int, d: Int): String = {
    val schema = inferType(e)
    schema.dataType match {
//      case Schema.TYPE_NUMERICAL => changeNumberFormat(e)
      case _ => M1(e, c, d)
    }
  }
  def M3(row: String, c: Int = -1, d: Int = -1): String = {
    row
//    val cols = row.split(',')
//    if(cols.length < 2) {
//      return row
//    }
//    val i = Random.nextInt(cols.length-1)
//    cols.slice(0, i+1).mkString(",") + "~" + cols.slice(i+1, cols.length).mkString(",")
  }
  def M4(e: String, c: Int, d: Int): String = {
    M1(e, c, d)
  }

  // input: a dataset row
  // returns new row with random column(s) dropped
  def M5(e: String, c: Int, d: Int): String = {
    M1(e, c, d)
//    val cols = e.split(',').to
//    val to_drop = (0 to Random.nextInt(this.max_col_drops)).map(_ => Random.nextInt(cols.length))
//    cols.zipWithIndex.filter{ case (_, i) => !to_drop.contains(i)}.map(_._1).mkString(",")
  }

  // input: a column value
  // returns an empty column (BigFuzz Paper)
  def M6(e: String, c: Int, d: Int): String = {
    M1(e, c, d)
  }

  def applySchemaAwareMutation(e: String, schema: Schema[Any], rand: Random): String = {
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob, rand)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob, rand)
      case _ if schema.range == null => mutateNumber(e, rand)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob, rand)
    }
  }

  def mutateCol(v: String, c: Int, d: Int): String = {
    val mutation_ids = Array(1, 2, 4, 5, 6).map(_-1)
    val probs = mutation_ids.map(Config.mutateProbs(_))
    val to_apply = MutationUtils.RouletteSelect(mutation_ids, probs)
    this.actual_app(to_apply) += 1
    probabalisticApply(this.mutations(to_apply), v, c, d)
  }

  def mutateRow(row: String, row_id: Int, dataset: Int, same_mut_locations: ListBuffer[ListBuffer[(Int, Int, Int)]]): String = {
    val nop_cols = same_mut_locations.flatten.filter{case (d, _, _) => d == dataset}.map{case (_, c, r) => Vector(c,r)}
    probabalisticApply(M3, row.split(',').zipWithIndex.map{
      case (e, i) if !nop_cols.contains(Vector(i,row_id)) =>
//        println(s"normal mutation applied to ($dataset, $i, $row_id)")
        mutateCol(e, i, dataset)
      case (e, _) => e
    }.mkString(","), prob=0.0f)
  }

  def mutate(dataset: Seq[String], d: Int, same_mut_locations: ListBuffer[ListBuffer[(Int,Int,Int)]]): Seq[String] = {
//   dataset.map(mutateRow(_, d, same_mut_locations))
    randomDuplications(dataset, this.max_row_dups, this.row_dup_prob).zipWithIndex
      .map{case (row, i) => mutateRow(randomDuplications(row.split(','), this.max_col_dups, this.col_dup_prob).mkString(","), i, d, same_mut_locations)}
  }

  def applyNormMutations(datasets: Array[Seq[String]], locs: ListBuffer[ListBuffer[(Int, Int, Int)]]): Array[Seq[String]] = {
    datasets.zipWithIndex.map{case (d, i) => mutate(d, i, locs)}
  }
  def mutator(data: Seq[String], d: Int, c: Int, r: Int, seed: Long): Seq[String] = {
    val rand = new Random(seed)
    val m_row = data(r).split(',')
    val schema = inferType(m_row(c))
    val equalized = "<testdummy>" //rand.nextString(m_row(c).length) // mostly produces non english characters
    m_row.update(c, applySchemaAwareMutation(if(flipCoin(0.1f,rand)) equalized else m_row(c), schema, rand))
    data.updated(r, m_row.mkString(","))
  }

  def mutator(data: Seq[String], d: Int, c: Int, r: Int, seed: Long, op: Operator): Seq[String] = {
    val rand = new Random(seed)
    println("==Debug Print===")
    println(data.mkString("\n"))
    println("=====")
    val m_row = data(r).split(',')
    val schema = inferType(m_row(c))

    // add support for different kinds of co-dependence here
    op.symbol match {
      case "==" =>
        val equalized = "<testdummy>" //rand.nextString(m_row(c).length) // mostly produces non english characters
        m_row.update(c, applySchemaAwareMutation(if (flipCoin(0.1f, rand)) equalized else m_row(c), schema, rand))
        data.updated(r, m_row.mkString(","))
      case _ =>
      m_row.update(c, applySchemaAwareMutation(m_row(c), schema, new Random()))
      data.updated(r, m_row.mkString(","))
    }
  }

//
//
//  def getSameMutLocs(join_relations: Array[Array[(Int, Array[Int], Array[Int])]], len_datasets: Array[Int]): Array[Array[(Int, Int, Int)]] = {
//    join_relations.map(
//      _.flatMap{
//        case (d, cols, rows) if cols.length > 1 =>
//          //          println(d, cols.toVector, rows.toVector)
//          rows.flatMap{r => cols.map((d, _, r))}
//        //          throw new Exception("mutation for multi-column joins not supported")
//        case (d, cols, rows) if rows.length == 0 => (0 until len_datasets(d)).toArray.map((d, cols(0), _))
//        case (d, cols, rows) => rows.map((d, cols(0), _))
//      }
//    )
//  }
//
//
  def stageMutations(provInfo: ProvInfo): Array[Array[Seq[String] => Seq[String]]] = {
    /*locs:
    [
       [(0, 0, 0), (1, 0, 0)],
       [(0, 1, 0), (1, 2, 0)],
    ]

    [
      [mutator1(0,0,0), mutator2(0,1,0)],
      [mutator1(1,0,0), mutator2(1,2,0)]
    ]
    */

    val locs = provInfo.getLocs()
    val mutations = locs.map {
      case CoDepTuple(op, regions) =>
        val seed = Random.nextLong()
        regions.map(_.getAsTuple).map {
          case (d, c, r) => data: Seq[String] => mutator(data, d, c, r, seed, op)
        }
    }

    val flattened = mutations.flatten
    locs
      .flatMap(_._2)
      .map(r => (r.ds, r.col, r.row))
      .zip(flattened) //pair staged mutations with locations
      .groupBy(_._1._1) // group by dataset
      .map(e => (e._1, e._2.map(_._2))) // (loc, List[(loc,mutator)]) -> (loc, List[mutator])
      .toArray
      .sortBy(_._1)
      .map(_._2.toArray) // sort by dataset and return only mutators
  }

  def applyStagedMutations(dataset: Seq[String], stagedMutations: Array[Seq[String] => Seq[String]]): Seq[String] = {
    stagedMutations.foldLeft(dataset)((mutated, mutation) => mutation(mutated))
  }


  def applyCoDependentMutations(inputDatasets: Array[Seq[String]], provInfo: ProvInfo): Array[Seq[String]] = {
//    println(provInfo)
    val stagedMutations = stageMutations(provInfo)
    inputDatasets.zipWithIndex.map{case (d, i) => applyStagedMutations(d, stagedMutations(i))}
  }

  def duplicateRow(datasets: Array[Seq[String]], loc: (Int, Int)): (Array[Seq[String]], (Int, Int)) = {
    val (ds, row) = loc
    println(s"=> d:$ds r:$row")
    (datasets.updated(ds, datasets(ds) :+ datasets(ds)(row)), (ds, datasets(ds).length))
  }

  def provenanceAwareDupication(inputDatasets: Array[Seq[String]], provInfo: ProvInfo, provInfoRand: ProvInfo, n: Int = 3): (Array[Seq[String]], ProvInfo) = {
    //(0,0,0)<=>(0,5,0)<=>(0,6,0)<=>(1,0,0)<=>(1,5,0)<=>(1,6,0)
    val toDuplicate = provInfoRand.getRowLevelProvenance() //(DS, ROW)

    //repeat this n times and flatten the resulting provInfoNew
    val (duplicatedDatasets, provInfoDuplicated) = (0 until n).foldLeft((inputDatasets, new ProvInfo())){
      case ((accDatasets, accProvInfo), _) =>
        val (datasetsWithNewRows, newLocs) = toDuplicate.foldLeft((accDatasets, Map[(Int, Int), (Int, Int)]())) {
          case ((datasets, newLocs), loc) =>
            val (newDatasets, newLoc) =  duplicateRow(datasets, loc)
            (newDatasets, newLocs + (loc -> newLoc))
        }
        val provInfoNew = provInfoRand.updateRowSet(newLocs)
        (datasetsWithNewRows, accProvInfo.append(provInfoNew).merge())
    }

    (duplicatedDatasets, provInfo.append(provInfoDuplicated))
  }

  def applyProvAwareMutation(inputDatasets: Array[Seq[String]], provInfo: ProvInfo): Array[Seq[String]] = {
    // perform guided duplication
    // perform co-dependent mutations
    // apply focused mutations (possibly same as above)
    val (m,n) = (3,3)
    val provInfoRand = provInfo.getRandom
    val (duplicated, provInfoDuplicated) = (0 until m)
      .foldLeft((inputDatasets, provInfo)){
        case ((accD, accP), _) =>
          provenanceAwareDupication(accD, accP, provInfoRand, n)
      }
    val mutated = applyCoDependentMutations(duplicated, provInfoDuplicated)
    applyNormMutations(mutated, provInfoDuplicated.getCoDependentRegions.map(_.regions.map(_.getAsTuple)))
  }

  def mutate(inputDatasets: Array[Seq[String]]): Array[Seq[String]] = {
    val mutatedDatasets = if(flipCoin(0.0001f))
      BFmutate(inputDatasets)
    else {
      applyProvAwareMutation(inputDatasets, provInfo)
    }
    mutatedDatasets
  }

  override def getInput(): Array[String] = {
    last_input
  }

  override def isDone(): Boolean = {
    !deadline.hasTimeLeft()
  }

  override def updateCoverage(cov: Coverage, outDir: String = "/dev/null", updateInternal: Boolean = true): Boolean = {
    var changed = false
    if (Global.iteration == 0 || cov.statementCoveragePercent > this.coverage.statementCoveragePercent) {
      if (updateInternal) {
        this.coverage = cov
      }
      changed = true;
    }
    changed
  }
}

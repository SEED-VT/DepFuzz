package testfiles

import fuzzer.{InstrumentedProgram, ProvInfo, Schema}

import scala.util.Random
import utils.MutationUtils._
import schemas.BenchmarkSchemas
import utils.{FileUtils, ProvFuzzUtils}


object Test1 {

  val schemas = BenchmarkSchemas.SEGMENTATION
  val mutate_probs = Array( // 0:M1, 1:M2 ... 5:M6
    1.0f, // Data
    1.0f, // Data
    0.1f, // Format
    1.0f, // Format
    0.1f, // Format
    0.1f) // Format

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
  val oor_prob = 0.1f //out-of-range probability: prob that a number will be mutated out of range vs normal mutation

  def applySchemaAwareMutation(e: String, schema: Schema[Any], rand: Random): String = {
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob, rand)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob, rand)
      case _ if schema.range == null => mutateNumber(e, rand)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob, rand)
    }
  }

  def mutator(data: Seq[String], d: Int, c: Int, r: Int, seed: Long): Seq[String] = {
    val rand = new Random(seed)
    val schema = this.schemas(d)(c)
    println(s"DATA: ${data(r)}")
    val m_row = data(r).split(',')
    m_row.update(c, applySchemaAwareMutation(m_row(c), schema, rand))
    data.updated(r, m_row.mkString(","))
  }


  def getSameMutLocs(join_relations: Array[Array[(Int, Array[Int], Array[Int])]], len_datasets: Array[Int]): Array[Array[(Int, Int, Int)]] = {
    join_relations.map(
      _.flatMap{
        case (d, cols, rows) if cols.length > 1 => rows.flatMap{r => cols.map((d, _, r))}
        case (d, cols, rows) if rows.length == 0 => (0 until len_datasets(d)).toArray.map((d, cols(0), _))
        case (d, cols, rows) => rows.map((d, cols(0), _))
      }
    )
  }

  def applyStagedMutations(dataset: Seq[String], staged_mutations: Array[Seq[String] => Seq[String]]): Seq[String] = {
    staged_mutations.foldLeft(dataset)((mutated, mutation) => mutation(mutated))
  }

  def stageMutations(locs: Array[Array[(Int, Int, Int)]]): Array[Array[Seq[String] => Seq[String]]] = {
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
    val mutations = locs.map{
      loc =>
        val seed = Random.nextLong()
        loc.map{
          case (d, c, r) => data: Seq[String] => mutator(data, d, c, r, seed)
      }
    }

    val flattened = mutations.flatten
    locs
      .flatten
      .zip(flattened)
      .groupBy(_._1._1)
      .map(e => (e._1, e._2.map(_._2)))
      .toArray
      .sortBy(_._1).map(_._2)
  }

  def applyJoinMutations(datasets: Array[Seq[String]], same_mut_locations: Array[Array[(Int, Int, Int)]]): Array[Seq[String]] = {
    val staged_mutations = stageMutations(same_mut_locations)
    datasets.zipWithIndex.map{case (d, i) => applyStagedMutations(d, staged_mutations(i))}
  }

  def M1(e: String, c: Int, d: Int): String = {
    val schema = this.schemas(d)(c)
    schema.dataType match {
      case Schema.TYPE_OTHER => mutateString(e, this.byte_mut_prob)
      case Schema.TYPE_CATEGORICAL => mutateString(e, this.byte_mut_prob)// schema.values(Random.nextInt(schema.values.length)).toString
      case _ if schema.range == null => mutateNumber(e)
      case Schema.TYPE_NUMERICAL => mutateNumberSchemaAware(e, schema, this.oor_prob)
    }
  }

  def M2(e: String, c: Int, d: Int): String = {
    val schema = this.schemas(d)(c)
    schema.dataType match {
      case Schema.TYPE_NUMERICAL => changeNumberFormat(e)
      case _ => e
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
    val to_apply = mutation_ids(Random.nextInt(mutation_ids.length))
    probabalisticApply(this.mutations(to_apply), v, c, d, this.mutate_probs(to_apply))
  }

  def mutateRow(row: String, dataset: Int, same_mut_locations: Array[Array[(Int, Int, Int)]]): String = {
    val nop_cols = same_mut_locations.flatten.filter{case (d, _, _) => d == dataset}.map{case (_, c, _) => c}
    probabalisticApply(M3, row.split(',').zipWithIndex.map{
      case (e, i) if !nop_cols.contains(i) => mutateCol(e, i, dataset)
      case (e, _) => e
    }.mkString(","), prob=this.mutate_probs(2))
  }

  def mutate(dataset: Seq[String], d: Int, same_mut_locations: Array[Array[(Int, Int, Int)]]): Seq[String] = {
    dataset.map(mutateRow(_, d, same_mut_locations))
  }

  def applyNormMutations(datasets: Array[Seq[String]], same_mut_locations: Array[Array[(Int, Int, Int)]]): Array[Seq[String]] = {
    datasets.zipWithIndex.map{case (d, i) => mutate(d, i, same_mut_locations)}
  }

  def duplicateRows(datasets: Array[Seq[String]],
                    mut_deps: Array[Array[(Int, Int, Int)]],
                    ds_cols: Array[(Int, Array[Int])]): (Array[Seq[String]], Array[Array[(Int, Int, Int)]]) = {

    val min_keys = 3
    val min_vals = 3
    //duplicate a random row from dataset for gbk fodder, will append to dataset later
    //below line returns (ds, <row to duplicate>)
    val ds_to_mutate = ds_cols.map(_._1).toSet
    val dup_rows = ds_to_mutate.map{
      case ds =>
        val mut_ds = datasets(ds)
        (ds, mut_ds(Random.nextInt(mut_ds.length)))
    }.toArray
    val gbk_fodder = dup_rows.map{case (ds, row) => ds -> (0 until min_keys * min_vals).map{_ => row}}.toMap

    //update dependencies

    val new_deps = (0 until min_keys).map {
      r =>
        dup_rows.flatMap {
          case (ds, _) =>
            val mut_ds = datasets(ds)
            (mut_ds.length + min_keys * r until mut_ds.length + min_keys * r + min_vals).flatMap{
              row =>
                val map_ds_cols = ds_cols.toMap
                map_ds_cols(ds).map(col => (ds, col, row))
            }
        }
    }.toArray
    //    val new_deps = (0 until min_keys).map{
    //      r =>
    //        (mut_ds.length + min_keys * r until mut_ds.length + min_keys * r + min_vals).flatMap {
    //          row =>
    //            ds_cols.map(col => (ds, col, row))
    //        }.toArray
    //    }.toArray

    // append new rows to datasets
    // append new dependencies to dependency list
    val new_datasets = datasets.zipWithIndex.map{
      case (ds, i) if gbk_fodder.contains(i) => ds ++ gbk_fodder(i)
      case (ds, _) => ds
    }
    (new_datasets, mut_deps ++ new_deps)
  }

  def guidedDuplication(datasets: Array[Seq[String]],
                        gbk_dependencies: Map[Int, Array[(Int, Array[Int])]],
                        same_mut_locations: Array[Array[(Int, Int, Int)]]): (Array[Seq[String]], Array[Array[(Int, Int, Int)]]) = {

    val new_ds = gbk_dependencies.foldLeft((datasets, same_mut_locations)){
      case ((dup_ds, mut_deps), (_, ds_cols)) =>
        duplicateRows(dup_ds, mut_deps, ds_cols)
    }
    new_ds
  }

  def main(args: Array[String]): Unit = {

  }
}

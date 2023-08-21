package utils

import fuzzer.Schema

import scala.collection.mutable
import scala.util.Random

object MutationUtils {
  def RouletteSelect[T](selection: Array[T], mutate_probs: Array[Float]): T = {
    assert(mutate_probs.foldLeft(0.0f)(_+_) <= 1.0f)
    val point = Random.nextFloat()

    val Some((_, v)) = mutate_probs
      .zip(selection)
      .sortBy(_._1)
      .foldLeft(Array((mutate_probs(0), selection(0)))){case (acc, (p, v)) => acc :+ (acc.last._1 + p, v)}
      .find{case (p, _) => point < p}
    v
  }

  def mutateChar(c: Char, prob: Float, rand: Random = Random): Char = {
    if(!flipCoin(prob, rand)) return c
    var gen = rand.nextPrintableChar()
    while(gen == ',') gen = rand.nextPrintableChar()
    gen
  }

  def mutateString(s: String, prob: Float = 0.5f, rand: Random = Random): String = {
    s.map(mutateChar(_, prob, rand))
  }

  def changeNumberFormat(num: String): String = {
    if(!num.replace(".", "").forall(_.isDigit) || num.isEmpty)
      return num
    try { num.toInt.toFloat.toString }
    catch { case _ => num.toFloat.toInt.toString }
  }

  def mutateNumber(v: String, rand: Random = Random): String = {
    try { (v.toInt + rand.nextInt(Int.MaxValue)).toString }
    catch { case _ => try { (v.toFloat + rand.nextInt(Int.MaxValue)).toString } catch { case _ => v }}
  }

  def mutateNumberSchemaAware(v: String, schema: Schema[Any], prob: Float, rand: Random = Random): String = {
    if(flipCoin(prob, rand)) {
      val (min, max) = schema.range
      return (Random.nextInt(max + 1 - min) + min).toString
    }
    mutateNumber(v, rand)
  }

  def probabalisticApply[T](f: (T, Int, Int) => T, arg: T, col: Int = -1, dataset: Int = -1, prob: Float = 1.0f): T = {
    if(flipCoin(prob)) f(arg, col, dataset) else arg
  }

  def randomDuplications[T](seq: Seq[T], max_dups: Int, prob: Float): Seq[T] = {
    if(!flipCoin(prob)) return seq
    val dup_indices = (0 to Random.nextInt(max_dups) - 1).map(_ => Random.nextInt(seq.length)).toSet.toArray
    val new_row = mutable.ListBuffer(seq:_*)
    dup_indices.foreach(dup_index => new_row.insert(dup_index, seq(dup_index)))
    new_row
  }

  // returns true with a probability of p
  def flipCoin(p: Float, rand: Random = Random): Boolean = {
    rand.nextFloat() < p
  }

}

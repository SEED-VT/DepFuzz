package taintedprimitives

import provenance.data.{DualRBProvenance, Provenance}
import scala.reflect.runtime.universe._

/**
  * Created by malig on 4/29/19.
  */
case class TaintedString(override val value: String, p: Provenance) extends TaintedAny(value, p) {

  /**
    * Unsupported Operations
    */

   def length: TaintedInt = {
     TaintedInt(value.length, getProvenance())
  }
  private def addColProv(p: Provenance, col: Int): Provenance = {
    p match {
      case _ : DualRBProvenance =>
        if(col > DualRBProvenance.MAX_COL_VAL) {
          throw new UnsupportedOperationException(s"Number of columns exceeded max allowed value of ${DualRBProvenance.MAX_COL_VAL} during call to split")
        }
        p // TODO: If we stick with instrumenting split for col prov, then the column prov will be added here using masking
      case _ => p
    }
  }

  def split(separator: Char): Array[TaintedString] = {
    var col = -1
     value
      .split(separator)
      .map(s => {
        col+=1
        TaintedString(
          s, addColProv(getProvenance(), col))
      })
  }
  def split(regex: String): Array[TaintedString] = {
    split(regex, 0)
  }

  def split(regex: String, limit: Int): Array[TaintedString] = {
    value
      .split(regex, limit)
      .map(s =>
        TaintedString(
          s, getProvenance()))
  }
   def split(separator: Array[Char]): Array[TaintedString] = {

    value
      .split(separator)
      .map(s =>
         TaintedString(
          s, getProvenance()
        ))
  }

  def substring(arg0: TaintedInt): TaintedString = {
      TaintedString(value.substring(arg0.value), newProvenance(arg0.getProvenance()))
  }

  def substring(arg0: Int, arg1: TaintedInt): TaintedString = {
    TaintedString(value.substring(arg0, arg1.value), newProvenance(arg1.getProvenance()))
  }
  def substring(arg0: TaintedInt, arg1: TaintedInt): TaintedString = {
    TaintedString(value.substring(arg0.value, arg1.value), newProvenance(arg0.getProvenance(), arg1.getProvenance()))
  }

  def substring(arg0: Int, arg1: Int): TaintedString = {
    TaintedString(value.substring(arg0, arg1), getProvenance())
  }

  def lastIndexOf(elem: Char): TaintedInt = {
    TaintedInt(value.lastIndexOf(elem), getProvenance())
  }
  
  def trim(): TaintedString = {
    TaintedString(value.trim, getProvenance())
  }

   def toInt: TaintedInt ={
    TaintedInt(value.toInt, getProvenance())
  }

   def toFloat: TaintedFloat =
     TaintedFloat(value.toFloat , getProvenance())

   def toDouble: TaintedDouble ={
    TaintedDouble(value.toDouble , getProvenance())
  }

  // TODO: add configuration to track equality checks, e.g. if used as a key in a map.
  def equals(obj: TaintedString): Boolean = value.equals(obj.value)
  def eq(obj: TaintedString): Boolean = value.eq(obj.value)

  def +(x: TaintedString): TaintedString = {
    TaintedString(value + x.value, getProvenance().merge(x.getProvenance()))
  }
  def +(x: String): TaintedString = {
    TaintedString(value + x, getProvenance())
  }

}

object TaintedString {
  implicit def lift = Liftable[TaintedString] { si =>
    q"(_root_.taintedprimitives.TaintedString(${si.value}, ${si.p}))"
  }
}
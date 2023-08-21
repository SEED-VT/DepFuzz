package taintedprimitives

import provenance.data.Provenance

object SymImplicits {

  //TODO: Using zero as default provenance here. We need to chain this disconnect through dependency analysis

  implicit def int2SymInt(s: Int): TaintedInt = new TaintedInt(s, Provenance.create())
  implicit def float2SymFloat(s: Float): TaintedFloat = TaintedFloat(s, Provenance.create())
  implicit def double2SymDouble(s: Double): TaintedDouble = TaintedDouble(s, Provenance.create())

  implicit def symVal2String(s: TaintedAny[_]): String = s.value.toString
//  implicit def symInt2String(s: TaintedInt): String = s.value.toString
//  implicit def symFloat2String(s: TaintedFloat): String = s.value.toString
//  implicit def symDouble2String(s: TaintedDouble): String = s.value.toString
  implicit def symString2String(s: TaintedString): String = s.value.toString

  implicit def symInt2SymFloat(s: TaintedInt): TaintedFloat = TaintedFloat(s.value, s.getProvenance())
  implicit def symFloat2SymInt(s: TaintedFloat): TaintedInt = TaintedInt(s.value.toInt, s.getProvenance())
  implicit def symFloat2SymDouble(s: TaintedFloat): TaintedDouble = TaintedDouble(s.value.toDouble, s.getProvenance())
  implicit def symInt2SymDouble(s: TaintedInt): TaintedDouble = TaintedDouble(s.value.toDouble, s.getProvenance())
  implicit def taintedBooleanToBoolean(s: TaintedBoolean): Boolean = s.value
  // A few common tuple options - these implicitly rely on the conversions defined above.
  type SymLong = TaintedInt //TODO we don't have a SymLong type yet, so for simplicity we use TaintedInt. This is *not* accurate.

  implicit def long2SymLong(s: Long): SymLong = TaintedInt(s.toInt, Provenance.create())
  // There are 16 definitions for pairs: 4 x 4.
  implicit def intIntTupleToSyms(tuple: (Int, Int)): (TaintedInt, TaintedInt) = (tuple._1, tuple._2)
  implicit def intLongTupleToSyms(tuple: (Int, Long)): (TaintedInt, SymLong) = (tuple._1, tuple._2)
  implicit def intDoubleTupleToSyms(tuple: (Int, Double)): (TaintedInt, TaintedDouble) = (tuple._1, tuple._2)

  implicit def intFloatTupleToSyms(tuple: (Int, Float)): (TaintedInt, TaintedFloat) = (tuple._1, tuple._2)
  implicit def longIntTupleToSyms(tuple: (Long, Int)): (SymLong, TaintedInt) = (tuple._1, tuple._2)
  implicit def longLongTupleToSyms(tuple: (Long, Long)): (SymLong, SymLong) = (tuple._1, tuple._2)
  implicit def longDoubleTupleToSyms(tuple: (Long, Double)): (SymLong, TaintedDouble) = (tuple._1, tuple._2)

  implicit def longFloatTupleToSyms(tuple: (Long, Float)): (SymLong, TaintedFloat) = (tuple._1, tuple._2)
  implicit def doubleIntTupleToSyms(tuple: (Double, Int)): (TaintedDouble, TaintedInt) = (tuple._1, tuple._2)
  implicit def doubleLongTupleToSyms(tuple: (Double, Long)): (TaintedDouble, SymLong) = (tuple._1, tuple._2)
  implicit def doubleDoubleTupleToSyms(tuple: (Double, Double)): (TaintedDouble, TaintedDouble) = (tuple._1, tuple._2)

  implicit def doubleFloatTupleToSyms(tuple: (Double, Float)): (TaintedDouble, TaintedFloat) = (tuple._1, tuple._2)
  implicit def floatIntTupleToSyms(tuple: (Float, Int)): (TaintedFloat, TaintedInt) = (tuple._1, tuple._2)
  implicit def floatLongTupleToSyms(tuple: (Float, Long)): (TaintedFloat, SymLong) = (tuple._1, tuple._2)
  implicit def floatDoubleTupleToSyms(tuple: (Float, Double)): (TaintedFloat, TaintedDouble) = (tuple._1, tuple._2)

  implicit def floatFloatTupleToSyms(tuple: (Float, Float)): (TaintedFloat, TaintedFloat) = (tuple._1, tuple._2)

  // Implicits are applied in order of priority, so these should be defined last so we try to use
  // symbolics as much as possible.
  implicit def symValToVal[T<: AnyVal](s: TaintedAny[T]): T = s.value
//  implicit def symInt2Int(s: TaintedInt): Int = s.value
//  implicit def symFloat2Float(s: TaintedFloat): Float = s.value
  //  implicit def symDouble2Double(s: TaintedDouble): Double = s.value
    implicit def symFloat2Double(s: TaintedFloat): Double = s.value.toDouble
}

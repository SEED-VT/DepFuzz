package taintedprimitives

import provenance.data.{DummyProvenance, Provenance}

import scala.reflect.runtime.universe._


/**
  * Created by malig on 4/25/19.
  */
case class TaintedDouble(i: Double, p: Provenance) extends TaintedAny(i, p) {
  def this(value: Double) = {
    this(value, DummyProvenance.create())
  }
  /**
    * Overloading operators from here onwards
    */

  def +(x: Double): TaintedDouble = {
    TaintedDouble(value + x, getProvenance())
  }

  def -(x: Double): TaintedDouble = {
    TaintedDouble(value - x, getProvenance())
  }

  def *(x: Double): TaintedDouble = {
    TaintedDouble(value * x, getProvenance())

  }

  def *(x: Float): TaintedDouble = {
    TaintedDouble(value * x, getProvenance())
  }

  def /(x: Double): TaintedDouble = {
    TaintedDouble(value / x, getProvenance())
  }

  def +(x: TaintedDouble): TaintedDouble = {
    TaintedDouble(value + x.value, newProvenance(x.getProvenance()))
  }

  def -(x: TaintedDouble): TaintedDouble = {
    TaintedDouble(value - x.value, newProvenance(x.getProvenance()))
  }

  def *(x: TaintedDouble): TaintedDouble = {
    TaintedDouble(value * x.value, newProvenance(x.getProvenance()))
  }

  def /(x: TaintedDouble): TaintedDouble = {
    TaintedDouble(value / x.value, newProvenance(x.getProvenance()))
  }

  def toInt: TaintedInt = {
    TaintedInt(value.toInt, getProvenance())
  }

  // TODO: Following are control flow provenance that, in my opinion, should be configurable. [Gulzar]
  // JT: agreed - for now, I've disabled since they don't return a new data type.
  def <(x: TaintedDouble): Boolean = {
    // mergeProvenance(x.getProvenance()) // see above note on configurable
    return value < x.value
  }

  def <(x: Double): Boolean = {
    return value < x
  }
  
  def >(x: TaintedDouble): Boolean = {
    // mergeProvenance(x.getProvenance()) // see above note on configurable
    return value > x.value
  }
  
  def >(x: Double): Boolean = {
    return value > x
  }

  def >=(x: TaintedDouble): Boolean = {
    // mergeProvenance(x.getProvenance()) // see above note on configurable
    value >= x.value
  }
  def <=(x: TaintedDouble): Boolean = {
    // mergeProvenance(x.getProvenance()) // see above note on configurable
    value <= x.value
  }

  def ==(x: Int): Boolean = {
    value == x
  }
  def >(x: Int): Boolean = {
    value > x
  }

  /**
    * Not Supported Symbolically yet
    **/
  //
  //  def toByte: Byte = value.toByte
  //
  //  def toShort: Short = value.toShort
  //
  //  def toChar: Char = value.toChar
  //
  //  def toInt: Int = value.toInt
  //
  //  def toLong: Long = value.toLong
  //
  //  def toFloat: Float = value.toFloat
  //
  //  def toDouble: Double = value.toDouble
  //
  //  def unary_~ : Int = value.unary_~
  //
  //  def unary_+ : Int = value.unary_+
  //
  //  def unary_- : Int = value.unary_-
  //
  //  def +(x: String): String = value + x
  //
  //  def <<(x: Int): Int = value << x
  //
  //  def <<(x: Long): Int = value << x
  //
  //  def >>>(x: Int): Int = value >>> x
  //
  //  def >>>(x: Long): Int = value >>> x
  //
  //  def >>(x: Int): Int = value >> x
  //
  //  def >>(x: Long): Int = value >> x
  //
  //  def ==(x: Byte): Boolean = value == x
  //
  //  def ==(x: Short): Boolean = value == x
  //
  //  def ==(x: Char): Boolean = value == x
  //
  //  def ==(x: Long): Boolean = value == x
  //
  //  def ==(x: Float): Boolean = value == x
  //
  //  def ==(x: Double): Boolean = value == x
  //
  //  def !=(x: Byte): Boolean = value != x
  //
  //  def !=(x: Short): Boolean = value != x
  //
  //  def !=(x: Char): Boolean = value != x
  //
  //  def !=(x: Int): Boolean = value != x
  //
  //  def !=(x: Long): Boolean = value != x
  //
  //  def !=(x: Float): Boolean = value != x
  //
  //  def !=(x: Double): Boolean = value != x
  //
  //  def <(x: Byte): Boolean = value < x
  //
  //  def <(x: Short): Boolean = value < x
  //
  //  def <(x: Char): Boolean = value < x
  //
  //  def <(x: Int): Boolean = value < x
  //
  //  def <(x: Long): Boolean = value < x
  //
  //  def <(x: Float): Boolean = value < x
  //
  //
  //
  //  def <=(x: Byte): Boolean = value <= x
  //
  //  def <=(x: Short): Boolean = value <= x
  //
  //  def <=(x: Char): Boolean = value <= x
  //
  //  def <=(x: Int): Boolean = value <= x
  //
  //  def <=(x: Long): Boolean = value <= x
  //
  //  def <=(x: Float): Boolean = value <= x
  //
  //  def <=(x: Double): Boolean = value <= x
  //
  //  def >(x: Byte): Boolean = value > x
  //
  //  def >(x: Short): Boolean = value > x
  //
  //  def >(x: Char): Boolean = value > x
  //
  //  def >(x: Int): Boolean = value > x
  //
  //  def >(x: Long): Boolean = value > x
  //
  //  def >(x: Float): Boolean = value > x
  //
  //  def >(x: Double): Boolean = value > x
  //
  //  def >=(x: Byte): Boolean = value >= x
  //
  //  def >=(x: Short): Boolean = value >= x
  //
  //  def >=(x: Char): Boolean = value >= x
  //
  //  def >=(x: Int): Boolean = value >= x
  //
  //  def >=(x: Long): Boolean = value >= x
  //
  //  def >=(x: Float): Boolean = value >= x
  //
  //  def >=(x: Double): Boolean = value >= x
  //
  //  def |(x: Byte): Int = value | x
  //
  //  def |(x: Short): Int = value | x
  //
  //  def |(x: Char): Int = value | x
  //
  //  def |(x: Int): Int = value | x
  //
  //  def |(x: Long): Long = value | x
  //
  //  def &(x: Byte): Int = value & x
  //
  //  def &(x: Short): Int = value & x
  //
  //  def &(x: Char): Int = value & x
  //
  //  def &(x: Int): Int = value & x
  //
  //  def &(x: Long): Long = value & x
  //
  //  def ^(x: Byte): Int = value ^ x
  //
  //  def ^(x: Short): Int = value ^ x
  //
  //  def ^(x: Char): Int = value ^ x
  //
  //  def ^(x: Int): Int = value ^ x
  //
  //  def ^(x: Long): Long = value ^ x
  //
  //  def +(x: Byte): Int = value + x
  //
  //  def +(x: Short): Int = value + x
  //
  //  def +(x: Char): Int = value + x
  //
  //  def +(x: Long): Long = value + x
  //
  //  def +(x: Float): Float = value + x
  //
  //  def +(x: Double): Double = value + x
  //
  //  def -(x: Byte): Int = value - x
  //
  //  def -(x: Short): Int = value - x
  //
  //  def -(x: Char): Int = value - x
  //
  //  def -(x: Long): Long = value - x
  //
  //  def -(x: Float): Float = value - x
  //
  //  def -(x: Double): Double = value - x
  //
  //  def *(x: Byte): Int = value * x
  //
  //  def *(x: Short): Int = value * x
  //
  //  def *(x: Char): Int = value * x
  //
  //  def *(x: Long): Long = value * x
  //
  //  def *(x: Float): Float = value * x
  //
  //  def *(x: Double): Double = value * x
  //
  //  def /(x: Byte): Int = value / x
  //
  //  def /(x: Short): Int = value / x
  //
  //  def /(x: Char): Int = value / x
  //
  //  def /(x: Long): Long = value / x
  //
  //  def /(x: Float): Float = value / x
  //
  //  def /(x: Double): Double = value / x
  //
  //  def %(x: Byte): Int = value % x
  //
  //  def %(x: Short): Int = value % x
  //
  //  def %(x: Char): Int = value % x
  //
  //  def %(x: Int): Int = value % x
  //
  //  def %(x: Long): Long = value % x
  //
  //  def %(x: Float): Float = value % x
  //
  //  def %(x: Double): Double = value % x
}

object TaintedDouble {
  implicit def ordering: Ordering[TaintedDouble] = Ordering.by(_.value)

  implicit def lift = Liftable[TaintedDouble] { si =>
    q"(_root_.taintedprimitives.TaintedDouble(${si.value}, ${si.p}))"
  }
}

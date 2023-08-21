package taintedprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.{DummyProvenance, Provenance}
import scala.reflect.runtime.universe._

case class TaintedInt(override val value: Int, p : Provenance) extends TaintedAny(value, p) {
  def this(value: Int) = {
    this(value, DummyProvenance.create())
  }
  
  /**
    * Overloading operators from here onwards
    */
  def +(x: Int): TaintedInt = {
    val d = value + x
    TaintedInt(d, getProvenance())
  }

  def -(x: Int): TaintedInt = {
    val d = value - x
    TaintedInt(d, getProvenance())
  }

  def *(x: Int): TaintedInt = {
    val d = value * x
    TaintedInt(d, getProvenance())
  }

  def *(x: Float): TaintedFloat = {
    val d = value * x
    TaintedFloat(d, getProvenance())
  }

  def /(x: Int): TaintedDouble = {
    val d = value / x
    TaintedDouble(d, getProvenance())
  }

  def /(x: Long): TaintedDouble= {
    val d = value / x
    TaintedDouble(d, getProvenance())
  }

  def /(x: TaintedInt): TaintedInt = {
    val d = value / x.value
    new TaintedInt(d, mergeProvenance(getProvenance(), x.getProvenance()))
  }


  def +(x: TaintedInt): TaintedInt = {
    TaintedInt(value + x.value, newProvenance(x.getProvenance()))
  }

  def -(x: TaintedInt): TaintedInt = {
    TaintedInt(value - x.value, newProvenance(x.getProvenance()))
  }

  def *(x: TaintedInt): TaintedInt = {
    TaintedInt(value * x.value, newProvenance(x.getProvenance()))
  }

  def %(x: Int): TaintedInt = {
    TaintedInt(value % x, getProvenance())
  }
  
  // Implementing on a need-to-use basis
  def toInt: TaintedInt = this
  def toDouble: TaintedDouble = TaintedDouble(value.toDouble, getProvenance())

  /**
    * Operators not supported yet
    */

  def ==(x: Int): Boolean = value == x

  def toByte: Byte = value.toByte

  def toShort: Short = value.toShort

  def toChar: Char = value.toChar

  

  def toLong: Long = value.toLong

  def toFloat: TaintedFloat = new TaintedFloat(value.toFloat, p)

  //def toDouble: Double = value.toDouble

  def unary_~ : Int = value.unary_~

  def unary_+ : Int = value.unary_+

  def unary_- : Int = value.unary_-

  def +(x: String): String = value + x

  def <<(x: Int): Int = value << x

  def <<(x: Long): Int = value << x

  def >>>(x: Int): Int = value >>> x

  def >>>(x: Long): Int = value >>> x

  def >>(x: Int): Int = value >> x

  def >>(x: Long): Int = value >> x

  def ==(x: Byte): Boolean = value == x

  def ==(x: Short): Boolean = value == x

  def ==(x: Char): Boolean = value == x

  def ==(x: Long): Boolean = value == x

  def ==(x: Float): Boolean = value == x

  def ==(x: Double): Boolean = value == x

  def !=(x: Byte): Boolean = value != x

  def !=(x: Short): Boolean = value != x

  def !=(x: Char): Boolean = value != x

  def !=(x: Int): Boolean = value != x

  def !=(x: Long): Boolean = value != x

  def !=(x: Float): Boolean = value != x

  def !=(x: Double): Boolean = value != x

  def <(x: Byte): Boolean = value < x

  def <(x: Short): Boolean = value < x

  def <(x: Char): Boolean = value < x

  def <(x: Int): Boolean = value < x
  def <(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value < x.value, newProvenance(x.getProvenance()))
  }

  def <(x: Long): Boolean = value < x

  def <(x: Float): Boolean = value < x

  def <(x: Double): Boolean = value < x

  def <=(x: Byte): Boolean = value <= x

  def <=(x: Short): Boolean = value <= x

  def <=(x: Char): Boolean = value <= x

  def <=(x: Int): Boolean = value <= x
  def <=(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value <= x.value, newProvenance(x.getProvenance()))
  }

  def <=(x: Long): Boolean = value <= x

  def <=(x: Float): Boolean = value <= x

  def <=(x: Double): Boolean = value <= x

  def >(x: Byte): Boolean = value > x

  def >(x: Short): Boolean = value > x

  def >(x: Char): Boolean = value > x

  def >(x: Int): Boolean = value > x
  def >(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value > x.value, newProvenance(x.getProvenance()))
  }

  def >(x: Long): Boolean = value > x

  def >(x: Float): Boolean = value > x

  def >(x: Double): Boolean = value > x

  def >=(x: Byte): Boolean = value >= x

  def >=(x: Short): Boolean = value >= x

  def >=(x: Char): Boolean = value >= x

  def >=(x: Int): Boolean = value >= x
  def >=(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value >= x.value, newProvenance(x.getProvenance()))
  }

  def >=(x: Long): Boolean = value >= x

  def >=(x: Float): Boolean = value >= x

  def >=(x: Double): Boolean = value >= x

  def |(x: Byte): Int = value | x

  def |(x: Short): Int = value | x

  def |(x: Char): Int = value | x

  def |(x: Int): Int = value | x

  def |(x: Long): Long = value | x

  def &(x: Byte): Int = value & x

  def &(x: Short): Int = value & x

  def &(x: Char): Int = value & x

  def &(x: Int): Int = value & x

  def &(x: Long): Long = value & x

  def ^(x: Byte): Int = value ^ x

  def ^(x: Short): Int = value ^ x

  def ^(x: Char): Int = value ^ x

  def ^(x: Int): Int = value ^ x

  def ^(x: Long): Long = value ^ x

  def +(x: Byte): Int = value + x

  def +(x: Short): Int = value + x

  def +(x: Char): Int = value + x

  def +(x: Long): Long = value + x

  def +(x: Float): Float = value + x

  def +(x: Double): Double = value + x

  def -(x: Byte): Int = value - x

  def -(x: Short): Int = value - x

  def -(x: Char): Int = value - x

  def -(x: Long): Long = value - x

  def -(x: Float): Float = value - x

  def -(x: Double): Double = value - x

  def *(x: Byte): Int = value * x

  def *(x: Short): Int = value * x

  def *(x: Char): Int = value * x

  def *(x: Long): Long = value * x

  def *(x: Double): Double = value * x

  def /(x: Byte): Int = value / x

  def /(x: Short): Int = value / x

  def /(x: Char): Int = value / x

  def /(x: Float): Float = value / x

  def /(x: Double): Double = value / x

  def %(x: Byte): Int = value % x

  def %(x: Short): Int = value % x

  def %(x: Char): Int = value % x

  def %(x: Long): Long = value % x

  def %(x: Float): Float = value % x

  def %(x: Double): Double = value % x

}

object TaintedInt {
  implicit def lift = Liftable[TaintedInt] { si =>
    q"(_root_.taintedprimitives.TaintedInt(${si.value}, ${si.p}))"
  }

  implicit def ordering: Ordering[TaintedInt] = Ordering.by(_.value)
}

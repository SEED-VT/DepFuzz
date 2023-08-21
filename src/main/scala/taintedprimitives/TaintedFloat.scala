package taintedprimitives

import provenance.data.Provenance

import scala.reflect.runtime.universe._

/**
  * Created by malig on 4/25/19.
  */

case class TaintedFloat(override val value: Float, p:Provenance) extends TaintedAny(value, p){
  def <(x: TaintedFloat): Boolean = {
    // mergeProvenance(x.getProvenance())
    value < x.value
  }

  /**
    * Overloading operators from here onwards
    */

  def +(x: Float): TaintedFloat = {
    val d = value + x
    TaintedFloat(d, getProvenance())
  }

  def -(x: Float): TaintedFloat = {
    val d = value - x
    TaintedFloat(d, getProvenance())
  }

  def *(x: Float): TaintedFloat = {
    val d = value * x
    TaintedFloat(d, getProvenance())

  }

  def *(x: Double): TaintedFloat = {
    val d = value * x
    TaintedFloat(d.toFloat, getProvenance())

  }

  def toFloat: TaintedFloat = {
    this
  }

  def /(x: Float): TaintedFloat = {
    val d = value / x
    TaintedFloat(d, getProvenance())
  }

  def +(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value + x.value, newProvenance(x.getProvenance()))
  }

  def +(x: TaintedDouble): TaintedDouble = {
    TaintedDouble(value + x.value, newProvenance(x.getProvenance()))
  }

  def -(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value - x.value, newProvenance(x.getProvenance()))
  }

  def *(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value * x.value, newProvenance(x.getProvenance()))

  }

  def /(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value / x.value, newProvenance(x.getProvenance()))
  }

  def /(x: Double): TaintedFloat = {
    // Very bad, should actually implement taintedouble but too lazy
    TaintedFloat((value / x).toFloat, getProvenance())
  }
  
  // Incomplete comparison operators - see discussion in TaintedDouble on provenance
  def >(x: TaintedFloat): Boolean = {
    // mergeProvenance(x.getProvenance())
    value > x.value
  }

  
  /**
    * Operators not Supported Symbolically yet
    **/
  override def toString: String =
    value.toString + s""" (Most Influential Input Offset: ${getProvenance()})"""
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
//  def <(x: Double): Boolean = value < x
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

object TaintedFloat {
  implicit def lift = Liftable[TaintedFloat] { si =>
    q"(_root_.taintedprimitives.TaintedFloat(${si.value}, ${si.p}))"
  }
}
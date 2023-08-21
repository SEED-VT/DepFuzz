package taintedprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.{DummyProvenance, Provenance}

import scala.reflect.runtime.universe._

case class TaintedBoolean(override val value: Boolean, p : Provenance) extends TaintedAny(value, p) {
  def this(value: Boolean) = {
    this(value, DummyProvenance.create())
  }
  
  /**
    * Overloading operators from here onwards
    */

  def &&(b: TaintedBoolean): TaintedBoolean = {
    TaintedBoolean(value && b.value, newProvenance(b.getProvenance()))
  }

  def ||(b: TaintedBoolean): TaintedBoolean = {
    TaintedBoolean(value && b.value, newProvenance(b.getProvenance()))
  }
}

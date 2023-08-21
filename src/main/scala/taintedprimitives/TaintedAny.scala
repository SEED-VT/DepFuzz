package taintedprimitives

import provenance.data.Provenance

abstract class TaintedAny[T <: Any](val value: T, p: Provenance) extends TaintedBase(p) {
  def toSymString: TaintedString = {
    TaintedString(value.toString, getProvenance())
  }

  override def hashCode(): Int = value.hashCode()
  
  override def equals(obj: scala.Any): Boolean =
    obj match {
      case x: TaintedAny[_] => value.equals(x.value)
      case _ => value.equals(obj)
    }


  override def toString: String = s"${this.getClass.getSimpleName}($value, ${getProvenance()})"
  
  
  // jteoh Disabled: These could actually be implicits in the symbase class too, but I'm hesitant
  // to do so because of scoping and incomplete knowledge on implicits.
  //  /** Creates new TaintedInt with current object provenance */
  //  def sym(v: Int): TaintedInt = TaintedInt(v, p)
  //
  //  // TODO: support Long type
  //  /** Creates new SymLong with current object provenance */
  //  def sym(v: Long): SymLong = new SymLong(v.toInt, p)
  //
  //  /** Creates new TaintedDouble with current object provenance */
  //  def sym(v: Double): TaintedDouble = TaintedDouble(v, p)
  //
  //  /** Creates new TaintedFloat with current object provenance */
  //  def sym(v: Float): TaintedFloat = TaintedFloat(v, p)
  //
  //  /** Creates new TaintedString with current object provenance */
  //  def sym(v: String): TaintedString = TaintedString(v, p)
}
object TaintedAny {
}

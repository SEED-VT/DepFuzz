package provenance.data

import scala.Ordering.Implicits._
object InfluenceMarker extends Enumeration {
  type InfluenceMarker = Value
  val left, right, both = Value
  
  type InfluenceFn[T] = (T, T) => Value
  /** a > b ? left : (a == b) ? both : right
    * need to define this as a no-param = function so that callers don't need to use `MaxFn _`
    * syntax */
  def MaxFn[T : Ordering]: InfluenceFn[T] =
    (a: T, b: T) => if (a > b) left else right
  
  def MaxWithTiesFn[T: Ordering]: InfluenceFn[T] =
    (a: T, b: T) => if (a > b) left else if (a == b) both else right
  
  def MinFn[T : Ordering]: InfluenceFn[T] = Invert(MaxFn)
  
  def MinWithTiesFn[T : Ordering]: InfluenceFn[T] = Invert(MaxWithTiesFn)
  
  def Invert[T](fn: (T, T) => Value): InfluenceFn[T] = {
    (a: T, b: T) => {
      val orig = fn(a, b)
      if (orig == left) right
      else if (orig == right) left
      else both
    }
  }
  
  
}
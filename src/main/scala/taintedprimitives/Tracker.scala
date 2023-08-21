package taintedprimitives

import org.roaringbitmap.RoaringBitmap

import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */

class Tracker[T:ClassTag](payload: T, rr: RoaringBitmap) extends Serializable {


  def value: T = return payload
  def bitmap: RoaringBitmap = return rr

  override def hashCode : Int = payload.hashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case str: Tracker[T] => this.equals(str)
      case _ =>  value.equals(obj)
    }
  }
   def equals(obj: Tracker[T]): Boolean = {
     this.payload.equals(obj.value)
   }


    override def toString: String = s""" ${payload.toString} --> ${rr.toString} Size: ${rr.getSizeInBytes} Bytes """
}


object Tracker{

//  def track[T:ClassTag, U:ClassTag](f: T => U): AnyTracker[T] => _ = {
//    println("Using normal wrapper")
//    return (a: AnyTracker[T]) => {
//      AnyTracker(f(a.payload),a.rr)
//    }
//  }

/* def track[T:ClassTag, K:ClassTag , V:ClassTag](f: T => (K,V)): AnyTracker[T] => (K, AnyTracker[V]) = {
    println("Using key-value wrapper")
    return (a: AnyTracker[T]) => {
      var  (key, value) = f(a.payload)
      (key, AnyTracker(value ,a.rr))
    }
  }
*/

//  def track[T:ClassTag, U:ClassTag](f: T => TraversableOnce[U]): AnyTracker[T] => TraversableOnce[AnyTracker[U]] = {
//    println("Using normal wrapper")
//    return (a: AnyTracker[T]) => {
//        f(a.payload).map(s => AnyTracker(s,a.rr))
//      }
//    }
}
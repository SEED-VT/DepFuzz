package sparkwrapper

import org.apache.spark.rdd.PairRDDFunctions
import org.roaringbitmap.RoaringBitmap
import sparkwrapper.WrappedRDD._
import taintedprimitives.Tracker

import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class WrappedPairRDD[K, V](rdd: PairRDDFunctions[K, Tracker[V]])(
    implicit kt: ClassTag[K],
    vt: ClassTag[V],
    ord: Ordering[K] = null)
    extends Serializable {
  def rankBitmaps(rr1: RoaringBitmap, rr2: RoaringBitmap): RoaringBitmap = {
    RoaringBitmap.or(rr1, rr2)
 //   rr1
  }
  // TODO: apply rank function here
  def reduceByKey(func: (V, V) => V): WrappedRDD[(K, V)] =
    return new WrappedRDD[(K, V)](rdd.reduceByKey { (v1, v2) =>
      val value = func(v1.value, v2.value)
      new Tracker(value,  rankBitmaps(v1.bitmap, v2.bitmap))
    })
}

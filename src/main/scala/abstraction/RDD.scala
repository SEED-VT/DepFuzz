package abstraction

import scala.reflect.ClassTag

trait RDD[T] {

  def map[U: ClassTag](f: T => U): RDD[U]

  def flatMap[U:ClassTag](f: T => TraversableOnce[U]): RDD[U]

  def filter(f: T => Boolean): RDD[T]

  def sortBy[K](f: (T) => K, ascending: Boolean = true)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]

  def reduce(f: (T, T) => T): T = ???

  def collect(): Array[T]

  def take(num: Int): Array[T]

  def setName(name: String): this.type

}

object RDD {
  implicit def toPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]): BasePairRDD[K,V] = {
    rdd match {
      case pair: BasePairRDD[K, V] => pair
      case rdd: BaseRDD[(K,V)] => new BasePairRDD(rdd.data)
      case _ => throw new NotImplementedError("Unknown RDD type for pair conversion: $rdd")
    }
  }
}


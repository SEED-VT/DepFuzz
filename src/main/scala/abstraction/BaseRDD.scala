package abstraction

import scala.reflect.ClassTag
import scala.util.Random

class BaseRDD[T:ClassTag](val data: Seq[T]) extends RDD[T]{
  def takeSample(bool: Boolean, n: Int): Array[T] = {
    new Random().shuffle(data).take(n).toArray
  }

  def filter(f:T=>Boolean): BaseRDD[T] = {
    new BaseRDD(data.filter(f))
  }

  override def reduce(f: (T, T) => T): T = {
    if(data.isEmpty)
      throw new UnsupportedOperationException()
    data.reduce(f)
  }

  def map[U:ClassTag](f: T => U): BaseRDD[U] = {
    new BaseRDD(data.map(f))
  }

  def collect(): Array[T] = {
    data.toArray
  }

  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = new BaseRDD(data.flatMap(f))

  override def take(num: Int): Array[T] = data.toArray

  override def setName(name: String): BaseRDD.this.type = ???

  override def sortBy[K](f: T => K, ascending: Boolean)
                        (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =
    new BaseRDD(data.sortBy(f))

  override def toString(): String = {
    data.mkString("\n")
  }
}


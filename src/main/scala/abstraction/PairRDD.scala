package abstraction

import scala.reflect.ClassTag

trait PairRDD[K, V] extends RDD[(K, V)] {

  def join[W](other: BasePairRDD[K, W]): PairRDD[K, (V, W)]

  def reduceByKey(f:(V,V) => V): PairRDD[K,V]

  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): PairRDD[K, U]

  def groupByKey(): RDD[(K, Seq[V])]

  def mapValues[U](f: V => U): RDD[(K, U)]

}

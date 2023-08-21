package abstraction
import scala.reflect.ClassTag
import scala.collection.mutable.{Map, HashMap}

class BasePairRDD[K, V](val data: Seq[(K, V)]) extends PairRDD[K, V] {
  override def map[U: ClassTag](f: ((K, V)) => U): RDD[U] = new BaseRDD(data.map(f))

  override def flatMap[U: ClassTag](f: ((K, V)) => TraversableOnce[U]): RDD[U] = new BaseRDD(data.flatMap(f))

  override def filter(f: ((K, V)) => Boolean): RDD[(K, V)] = new BasePairRDD(data.filter(f))

  def join[W](other: BasePairRDD[K, W]): PairRDD[K, (V, W)] = {
    var joined = Vector[(K, (V, W))]()
    var kmap = new scala.collection.mutable.HashMap[K, Vector[V]]
    for ((k, v) <- data) {
      if (kmap.contains(k))
        kmap(k) = kmap(k) :+ v
      else
        kmap(k) = Vector(v)
    }
    for ((k, w) <- other.data)
      if (kmap.contains(k)) {
        for (v <- kmap(k).toList) {
          joined = joined :+ ((k, (v, w)))
        }
      }
    new BasePairRDD(joined.toSeq)
  }

  def reduceByKey(f:(V,V) => V): PairRDD[K,V] = {
    new BasePairRDD(data.groupBy(_._1).map(k => (k._1, k._2.map(_._2))).map{e => (e._1,e._2.reduce(f.asInstanceOf[(Any, Any) => Any]).asInstanceOf[V])}.toSeq)
  }

  override def groupByKey(): PairRDD[K, Seq[V]] = {
    new BasePairRDD(data.groupBy(_._1).map(k => (k._1, k._2.map(_._2))).toSeq)
  }

  override def collect(): Array[(K, V)] = data.toArray

  override def take(num: Int): Array[(K, V)] = data.take(3).toArray

  override def setName(name: String): BasePairRDD.this.type = ???

  override def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): PairRDD[K, U] = {
    new BasePairRDD(data.groupBy(_._1).map(k => (k._1, k._2.map(_._2))).map{e => (e._1,e._2.aggregate(zeroValue)(seqOp,combOp))}.toSeq)
  }

  override def mapValues[U](f: V => U): RDD[(K, U)] = {
    new BasePairRDD(data.map(v => (v._1, f(v._2))))
  }

  override def sortBy[O](f: ((K, V)) => O, ascending: Boolean)
                        (implicit ord: Ordering[O], ctag: ClassTag[O]): RDD[(K, V)] =
    new BasePairRDD(data.sortBy(f))
}

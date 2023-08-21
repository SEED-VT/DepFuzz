package provenance.rdd

import provenance.data.Provenance

class ProvenanceGrouping[T](data: Iterable[ProvenanceRow[T]]) extends TraversableOnce[T] with Serializable {
  // collection-like object that tracks provenance. Due to engineering effort, this implements a
  // minimal subset of operations required for research, but in practice it should ideally
  // implement its own Iterable interface.
  private def values: Iterable[T] = data.map(_._1)
  // Returns
  def map[U](fn: T => U): ProvenanceGrouping[U] = {
    new ProvenanceGrouping[U](data.map(row => (fn(row._1), row._2)))
  }

  def combinedProvenance: Provenance = {
    // TODO: check for missing data
    val base = data.head._2.cloneProvenance()
    data.map(_._2).foldLeft(base)(_.merge(_))
    //data.foreach(x => base.merge(x._2))
    //base
  }

  override def size(): Int = data.size

  def applyFunctionToData[U](fn: Iterable[T] => U) = {
    fn(data.map(_._1))
  }

  def getData: Iterable[ProvenanceRow[T]] = data

  override def isEmpty: Boolean = data.isEmpty

  override def hasDefiniteSize: Boolean = data.hasDefiniteSize

  override def seq: TraversableOnce[T] = this

  // Functions considered unsupported w.r.t. provenance
  override def foreach[U](f: T => U): Unit = ???

  override def forall(p: T => Boolean): Boolean = ???

  override def exists(p: T => Boolean): Boolean = ???

  // Would need an Option-equivalent with provenance
  override def find(p: T => Boolean): Option[T] = ???

  override def copyToArray[B >: T](xs: Array[B], start: Int, len: Int): Unit = ???

  override def toTraversable: Traversable[T] = ???

  override def isTraversableAgain: Boolean = ???

  override def toStream: Stream[T] = ???

  override def toIterator: Iterator[T] = ???
}
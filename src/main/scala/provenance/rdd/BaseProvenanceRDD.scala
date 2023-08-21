package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class BaseProvenanceRDD[T : ClassTag](override val rdd: RDD[_]) extends ProvenanceRDD[T] {
  
  /** Specialized flatMap to detect if a ProvenanceGrouping is used. */
  // def flatMap[U: ClassTag](f: T => ProvenanceGrouping[U]): FlatProvenanceDefaultRDD[U]
  
//  final def distinct(): ProvenanceRDD[T] = this.distinct(baseRDD.getNumPartitions)

  final def persist(newLevel: StorageLevel): this.type = {
    rdd.persist(newLevel)
    this
  }

  final def persist(): this.type = {
    rdd.persist()
    this
  }

  final def unpersist(blocking: Boolean): this.type = {
    rdd.unpersist(blocking)
    this
  }

  final def cache(): this.type = {
    rdd.cache()
    this
  }

  final def setName(name: String): this.type = {
    rdd.setName(name)
    this
  }

}


package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner}
import provenance.data.InfluenceMarker._

import scala.reflect.ClassTag

trait PairProvenanceRDD[K, V] extends ProvenanceRDD[(K, V)] {
  // Require that this trait is only mixed into ProvenanceRDDs
  this: ProvenanceRDD[(K, V)] =>

  val kct: ClassTag[K]
  val vct: ClassTag[V]

  def defaultPartitioner: Partitioner

  def values: BaseProvenanceRDD[V]
  def mapValues[U: ClassTag](
      f: V => U,
      enableUDFAwareProv: Option[Boolean] = None): PairProvenanceRDD[K, U]

  def combineByKeyWithClassTagOld[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner = defaultPartitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null,
      enableUDFAwareProv: Option[Boolean] = None,
      inflFunction: Option[(V, V) => InfluenceMarker] = None)(
      implicit ct: ClassTag[C]): PairProvenanceRDD[K, C]

  def combineByKeyWithClassTag[C](
     createCombiner: V => C,
     mergeValue: (C, V) => C,
     mergeCombiners: (C, C) => C,
     partitioner: Partitioner = defaultPartitioner,
     mapSideCombine: Boolean = true,
     serializer: Serializer = null,
     enableUDFAwareProv: Option[Boolean] = None,
     influenceTrackerCtr: Option[() => InfluenceTracker[V]] = None)(
     implicit ct: ClassTag[C]): PairProvenanceRDD[K, C]

  def reduceByKey(
      func: (V, V) => V,
      enableUDFAwareProv: Option[Boolean] = None): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag(identity,
                             func,
                             func,
                             enableUDFAwareProv = enableUDFAwareProv)(vct)
  }

  def monitoredReduceByKey(
                   func: (V, V) => V,
                   enableUDFAwareProv: Option[Boolean] = None): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag(identity,
      func,
      func,
      enableUDFAwareProv = enableUDFAwareProv)(vct)
  }

  /** If Influence function is given then the UDFAwareProvenance is un-applicable */
  def reduceByKeyOld(
      func: (V, V) => V,
      influence: (V, V) => InfluenceMarker): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTagOld(identity,
                             func,
                             func,
                             enableUDFAwareProv = Some(false),
                             inflFunction = Some(influence))(vct)
  }

  def reduceByKey(
      func: (V, V) => V,
      influence: () => InfluenceTracker[V]): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag(identity,
                             func,
                             func,
                             enableUDFAwareProv = Some(false),
                             influenceTrackerCtr = Some(influence))(vct)
  }


  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def reduceByKey(func: (V, V) => V,
                  numPartitions: Int,
                  enableUDFAwareProv: Option[Boolean]): PairProvenanceRDD[K, V] = {
    reduceByKey(new HashPartitioner(numPartitions), func, enableUDFAwareProv)
  }

  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def reduceByKey(partitioner: Partitioner,
                  func: (V, V) => V, enableUDFAwareProv: Option[Boolean]): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner,
                                enableUDFAwareProv = enableUDFAwareProv)(vct)
  }

  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
                                                                          combOp: (U, U) => U,
                                                                          enableUDFAwareProv: Option[Boolean],
                                                                          influenceTrackerCtr: Option[() => InfluenceTracker[V]]
  ): PairProvenanceRDD[K, U]

  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
                                                                    combOp: (U, U) => U,
                                                                    enableUDFAwareProv: Option[Boolean],
                                                                    influenceTrackerCtr: Option[() => InfluenceTracker[V]]
  ): PairProvenanceRDD[K, U] = {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp,
                                                                  enableUDFAwareProv, influenceTrackerCtr)
  }

  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U,
                                                enableUDFAwareProv: Option[Boolean] = None,
                                                influenceTrackerCtr: Option[() => InfluenceTracker[V]] = None
  ): PairProvenanceRDD[K, U] = {
    aggregateByKey(zeroValue, defaultPartitioner)(seqOp, combOp, enableUDFAwareProv, influenceTrackerCtr)
  }

  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def aggregateByKeyOld[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
                                                                          combOp: (U, U) => U,
                                                                          enableUDFAwareProv: Option[Boolean],
                                                                          inflFunction: Option[(V, V) => InfluenceMarker]
  ): PairProvenanceRDD[K, U]

  // Note: enableUDFAwareProv is not provided a default value here because of
  // currying/overloading issues in Scala that conflict with the other aggregateByKey methods.
  // The default value should be None.
  def aggregateByKeyOld[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
                                                                    combOp: (U, U) => U,
                                                                    enableUDFAwareProv: Option[Boolean],
                                                                    inflFunction: Option[(V, V) => InfluenceMarker]
                                                                    ): PairProvenanceRDD[K, U] = {
    aggregateByKeyOld(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp,
                                                                  enableUDFAwareProv, inflFunction)
  }

  def aggregateByKeyOld[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U,
                                                enableUDFAwareProv: Option[Boolean] = None,
                                                inflFunction: Option[(V, V) => InfluenceMarker] = None
  )
  : PairProvenanceRDD[K, U] = {
    aggregateByKeyOld(zeroValue, defaultPartitioner)(seqOp, combOp, enableUDFAwareProv,
                                                     inflFunction)
  }

  def groupByKey() : PairProvenanceDefaultRDD[K, Iterable[V]]

//
//  def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, ProvenanceGrouping[V]]
//  def groupByKey(numPartitions: Int): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(new HashPartitioner(numPartitions))
//  }
//  def groupByKey(): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(defaultPartitioner)
//  }

  def join[W](other: PairProvenanceDefaultRDD[K, W],
              partitioner: Partitioner = defaultPartitioner
             ): PairProvenanceRDD[K, (V, W)]


  implicit class RDDWithDataSource(rdd: RDD[_]) {
    def firstSource: RDD[_] = {
      rdd.allSources.head
    }

    def allSources: Seq[RDD[_]] = {
      if (rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(_.allSources).distinct
      }
    }
  }

}

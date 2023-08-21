package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner, SparkEnv}
import provenance.data.InfluenceMarker._
import provenance.data.{DummyProvenance, Provenance}
import taintedprimitives.Utils
import org.apache.spark.util.collection.CompactBuffer

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.math
import scala.reflect.ClassTag

class PairProvenanceDefaultRDD[K, V](override val rdd: RDD[(K, ProvenanceRow[V])])(
    implicit val kct: ClassTag[K],
    implicit val vct: ClassTag[V])
    extends BaseProvenanceRDD[(K, V)](rdd)
    with PairProvenanceRDD[K, V] {

  override def defaultPartitioner: Partitioner =
    org.apache.spark.Partitioner.defaultPartitioner(rdd)

  private def flatRDD: RDD[ProvenanceRow[(K, V)]] = {
    rdd.map({
      case (k, (v, prov)) => ((k, v), prov)
    })
  }

  private def flatProvenanceRDD: FlatProvenanceDefaultRDD[(K, V)] = {
    FlatProvenanceDefaultRDD
      .pairToFlat(this)
  }

  override def values: FlatProvenanceDefaultRDD[V] = {
    new FlatProvenanceDefaultRDD[V](rdd.values)
  }

  override def map[U: ClassTag](
      f: ((K, V)) => U,
      enableUDFAwareProv: Option[Boolean] = None): FlatProvenanceDefaultRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.map({
      case (k, (v, prov)) => (f((k, v)), prov)
    }))
  }


  override def mapValues[U: ClassTag](
      f: V => U,
      enableUDFAwareProv: Option[Boolean] = None): PairProvenanceDefaultRDD[K, U] = {
    val _enableUDFAwareProv = Utils.getUDFAwareEnabledValue(enableUDFAwareProv)
    new PairProvenanceDefaultRDD(rdd.mapValues({
      case (v, prov) =>
        Utils.computeOneToOneUDF(f, (v, prov), _enableUDFAwareProv)
    }))
  }

  override def flatMap[U: ClassTag](
      f: ((K, V)) => TraversableOnce[U],
      enableUDFAwareProv: Option[Boolean] = None): FlatProvenanceDefaultRDD[U] = {
    val _enableUDFAwareProv = Utils.getUDFAwareEnabledValue(enableUDFAwareProv)
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.flatMap({
      // TODO this might be slow, one optimization is to have a classTag on the return type and
      // check that ahead of time before creating the UDF
      case (k, (v, prov)) => {
        Utils.computeOneToManyUDF(f, ((k, v), prov), _enableUDFAwareProv)
      }
    }))
  }

  /** Specialized flatMap to detect if a ProvenanceGrouping is used. */
//  override def flatMap[U: ClassTag](f: ((K, V)) => ProvenanceGrouping[U]): FlatProvenanceDefaultRDD[U] = {
//    // If a provenance grouping is returned, we should expect to flatten it ourselves and split
//    // up the provenance accordingly.
//    // There's an unstated assumption here that the arguments (K, V) contain a base
//    // provenance grouping and an operation such as map() is being called on them.
//    // As a result, the provided provenance is unused (e.g. it may have been the merged
//    // provenance for the entire ProvenanceGrouping, used as a placeholder in case it's needed
//    // later).
//    new FlatProvenanceDefaultRDD(rdd.flatMap({
//      case (k, (v, unusedProvenance)) => f((k, v)).asIterable
//    }))
//  }

  override def filter(f: ((K, V)) => Boolean): ProvenanceRDD[(K, V)] = {
    // filter doesn't require us to remap anything, so keep it as a pair rdd
    new PairProvenanceDefaultRDD(rdd.filter({
      case (k, (v, _)) => f((k, v))
    }))
  }
//
//  override def distinct(numPartitions: Int)
//                       (implicit ord: Ordering[(K, V)]): ProvenanceRDD[(K, V)
//  ] = map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
//
  override def collect(): Array[(K, V)] = flatProvenanceRDD.collect()
//
  override def collectWithProvenance(): Array[((K, V), Provenance)] =
    flatProvenanceRDD.collectWithProvenance()

  override def take(num: Int): Array[(K, V)] = flatProvenanceRDD.take(num)

  override def takeWithProvenance(num: Int): Array[((K, V), Provenance)] =
    flatProvenanceRDD.takeWithProvenance(num)
//
//  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[(K, V)] =
//    flatProvenanceRDD.takeSample(withReplacement, num, seed)
//
//  override def takeSampleWithProvenance(withReplacement: Boolean, num: Int, seed: Long): Array[((K, V), Provenance)] =
//    flatProvenanceRDD.takeSampleWithProvenance(withReplacement, num, seed)
//
//  /**
//    * Optimized combineByKey implementation where provenance tracking data structures are
//    * constructed only once per partition+key.
//    */


  /**
    * [Gulzar]
    * Ugly implementation of this combiner method. Following are the assumptions:
    *   1. UDFAwareProvenance is disabled by default (see Utils.getUDFAwareEnabledValue)
    *      Will only be performed when the output of UDF is SYM*Object and the flag is enabled.
    *      if enabled, the influence function will be disabled (overridden) in favor of symbolic
    *      object provenance.
    *   2. Influence function will only work when UDFAwareProvenance is disabled.
    *
    *   Our modified create Combiner method (Defined in Utils) create a combiner
    *   object that contains the Combined output, the most influential source value
    *   (random value if influential function is not given), and prov of most
    *   influence/logical lineage.
    *
    *   CombinerWithInfluence --> ( (C , V), Prov)
    *   TODO UPDATE DOC
    *
    * TODO if output type is symbolic, adjust provenance accordingly
    * */
  override def combineByKeyWithClassTagOld[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner = defaultPartitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null,
      enableUDFAwareProv: Option[Boolean] = None,
      inflFunction: Option[(V, V) => InfluenceMarker] = None)(
      implicit ct: ClassTag[C]): PairProvenanceDefaultRDD[K, C] = {
    val _enableUDFAwareProv = Utils.getUDFAwareEnabledValue(enableUDFAwareProv)
    assert(inflFunction.isEmpty || !_enableUDFAwareProv, "UDFAware Provenance " +
      "should not be enabled if using influence functions")
    // Based on ShuffledRDD implementation for serializer
    val resultSerializer = serializer
    // shorthands for easier reference
    type ValueRow = ProvenanceRow[V]
    type CombinerRow = ProvenanceRow[CombinerWithInfluence[C,V]]


    val createProvCombiner: ValueRow => CombinerRow =
      (valueRow: ValueRow) =>
        Utils.createCombinerForReduce(createCombiner,valueRow._1,valueRow._2.cloneProvenance(), _enableUDFAwareProv)

    val mergeProvValue: (CombinerRow, ValueRow) => (CombinerRow) =
      (combinerRow: CombinerRow, valueRow: ValueRow) => {
        Utils.computeCombinerWithValueUDF(mergeValue,
                                          combinerRow,
                                          valueRow,
                                          _enableUDFAwareProv,
                                          inflFunction)
      }

    val mergeProvCombiners: (CombinerRow, CombinerRow) => CombinerRow =
      (combinerRow1: CombinerRow, combinerRow2: CombinerRow) => {
        Utils.computeCombinerWithCombinerUDF[C,V](mergeCombiners,
                                                  combinerRow1,
                                                  combinerRow2,
                                                  _enableUDFAwareProv,
                                                  inflFunction)
      }

    new PairProvenanceDefaultRDD[K, C](
      {
        val combinerResult: RDD[(K, CombinerRow)] = rdd.combineByKeyWithClassTag[CombinerRow](
          // init: create a new 'tracker' instance that we can reuse for all values in the key.
          // TODO existing bug: cloning provenance is expensive and should be done lazily...
          createProvCombiner,
          mergeProvValue,
          mergeProvCombiners,
          partitioner,
          mapSideCombine,
          resultSerializer
          )

         // Key, combiner, provenance (the influence marker is not propagated yet)
         //.map(row => (row._1 , (row._2._1._1, row._2._2)))
         combinerResult.mapValues((row: CombinerRow) => (row._1._1, row._2))
      }
    )
  }

  // TODO test new api, integrate if functional
  override def combineByKeyWithClassTag[C](
                                     createCombiner: V => C,
                                     mergeValue: (C, V) => C,
                                     mergeCombiners: (C, C) => C,
                                     partitioner: Partitioner = defaultPartitioner,
                                     mapSideCombine: Boolean = true,
                                     serializer: Serializer = null,
                                     enableUDFAwareProv: Option[Boolean] = None,
                                     influenceTrackerCtr: Option[() => InfluenceTracker[V]] = None)(
                                     implicit ct: ClassTag[C]): PairProvenanceDefaultRDD[K, C] = {
    val _enableUDFAwareProv = Utils.getUDFAwareEnabledValue(enableUDFAwareProv)
    assert(influenceTrackerCtr.isEmpty || !_enableUDFAwareProv, "UDFAware Provenance " +
      "should not be enabled if using influence functions")
    // Based on ShuffledRDD implementation for serializer
    val resultSerializer = serializer
    // shorthands for easier reference
    type ValueRow = ProvenanceRow[V]

    if(_enableUDFAwareProv) {
      // implicit assumption that there is some sort of taint available in the data record

      // We'll use the combiner's provenance since it's a symbase. No need to propagate row-level
      // provenance! This of course means we also may lose information if the taint objects do
      // not work as intended, but that is an expectation for this flag being enabled.
      def createProvCombiner(value: ValueRow): C = createCombiner(value._1)
      def mergeProvValue(combiner: C, value: ValueRow): C = mergeValue(combiner, value._1)
      // use default mergeCombiners
      val mergeProvCombiners = mergeCombiners

      val combinerResult: RDD[(K, C)] = rdd.combineByKeyWithClassTag[C](
        createProvCombiner _,
        mergeProvValue _,
        mergeProvCombiners,
        partitioner,
        mapSideCombine,
        resultSerializer
        )
      // The output should contain a symobj, so rely on that to identify provenance
      // v.asInstanceOf[TaintedBase].getProvenance()
      // WARNING - if there is no provenance to infer, this can actually be terribly expensive.
      val extractedSymBaseProv = combinerResult.mapValues(v => (v, Utils.inferProvenance(v)))
      new PairProvenanceDefaultRDD[K,C](extractedSymBaseProv)
    } else {
      // default is an AllInfluenceTracker, i.e. don't filter anything.
      val _influenceTrackerCtr: () => InfluenceTracker[V] = influenceTrackerCtr.getOrElse(AllInfluenceTracker[V])
      // We should use influence functions. These will be tied along with each combiner to
      // identify what the retained provenance should be.
      // jteoh 8/24/20: If influence functions are used with symbolic data types, we need to
      // manually zero out the existing provenance and capture it again after...
      // This involves two steps:
      // (1): map all data records (both key and value) and *replace* (update?).
      // (2): after combine by key, when extracting tracker provenance to row-level, also update
      // the row's data values (searching for symbolic)


      type CombinerWithInfluenceTracker = (C, InfluenceTracker[V])
      def createProvCombiner(value: ValueRow): CombinerWithInfluenceTracker = {
        val tracker = _influenceTrackerCtr() // needed for compile for unknown reasons...
        tracker.init(value)
        (createCombiner(value._1), tracker)
      }
      def mergeProvValue(combinerRow: CombinerWithInfluenceTracker, value: ValueRow): CombinerWithInfluenceTracker = {
        (mergeValue(combinerRow._1, value._1), combinerRow._2.mergeValue(value))
      }
      def mergeProvCombiners(c1: CombinerWithInfluenceTracker, c2: CombinerWithInfluenceTracker): CombinerWithInfluenceTracker = {
        (mergeCombiners(c1._1, c2._1), c1._2.mergeTracker(c2._2))
      }

      // Step 1 of managing taints in influence functions
      val baseRdd = rdd.map(row => {
        // minor simplification: we'll apply on the whole row including the row provenance
        // itself, which should be ignored by this function since it only targets symbolic taint
        // objects.
        // TODO is it necessary to 'zero out' the provenance for the keys? or just the values?
        Utils.replaceSymProvenance(row, DummyProvenance.create())
      })
      val combinerResult: RDD[(K, CombinerWithInfluenceTracker)] =
        baseRdd.combineByKeyWithClassTag[CombinerWithInfluenceTracker](
          createProvCombiner _,
          mergeProvValue _,
          mergeProvCombiners _,
          partitioner,
          mapSideCombine,
          resultSerializer
        )
      // Step 2 of managing taints in influence functions: adopt the tracker provenance into any
      // symbolic values present.
      //val combinerRowResults: RDD[(K, ProvenanceRow[C])] = combinerResult.mapValues(
      //  {case (combiner, tracker) => (combiner, tracker.computeProvenance())})
      val combinerRowResults: RDD[(K, ProvenanceRow[C])] = combinerResult.map(
        {case (key, (combiner, tracker)) =>
          val trackerProv = tracker.computeProvenance()
          val newKey = Utils.replaceSymProvenance(key, trackerProv)
          val newCombiner = Utils.replaceSymProvenance(combiner, trackerProv)
          (newKey, (newCombiner, trackerProv))
        })
//      println("DEBUGGING")
//      val test = rdd.take(1)
//      val testResult = Utils.replaceSymProvenance(test, DummyProvenance.create())
//      rdd.take(1).foreach(println)
//      baseRdd.take(1).foreach(println)


      new PairProvenanceDefaultRDD[K,C](combinerRowResults)

    }
  }

  override def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)
                                             (seqOp: (U, V) => U,
                                              combOp: (U, U) => U,
                                              enableUDFAwareProv: Option[Boolean],
                                              influenceTrackerCtr: Option[() => InfluenceTracker[V]])
  : PairProvenanceRDD[K,U] = {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = seqOp // TODO: clean closure
    // rdd.context.clean(seqOp)
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
                                   cleanedSeqOp, combOp, partitioner, enableUDFAwareProv = enableUDFAwareProv,
                                   influenceTrackerCtr = influenceTrackerCtr)
  }

 /**
   * Moving from jteoh branch
   *
   * */
 override def aggregateByKeyOld[U: ClassTag](zeroValue: U, partitioner: Partitioner)
                                         (seqOp: (U, V) => U,
                                          combOp: (U, U) => U,
                                          enableUDFAwareProv: Option[Boolean],
                                          inflFunction: Option[InfluenceFn[V]])
 : PairProvenanceRDD[K,U] = {
   // Serialize the zero value to a byte array so that we can get a new clone of it on each key
   val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
   val zeroArray = new Array[Byte](zeroBuffer.limit)
   zeroBuffer.get(zeroArray)

   lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
   val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

   // We will clean the combiner closure later in `combineByKey`
   val cleanedSeqOp = seqOp // TODO: clean closure
   // rdd.context.clean(seqOp)
   combineByKeyWithClassTagOld[U]((v: V) => cleanedSeqOp(createZero(), v),
     cleanedSeqOp, combOp, partitioner, enableUDFAwareProv = enableUDFAwareProv,
                               inflFunction = inflFunction)
 }


  // END: Additional Spark-supported reduceByKey APIs

  // An alternate GBK that produces a specialized 'iterable' which internally tracks fine-grained
  // provenance.
//  override def groupByKey(partitioner: Partitioner): PairProvenanceGroupingRDD[K, V] =  {
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: ProvenanceRow[V]) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[ProvenanceRow[V]], v: ProvenanceRow[V]) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[ProvenanceRow[V]], c2: CompactBuffer[ProvenanceRow[V]]) => c1 ++= c2
//    val bufs: RDD[(K, CompactBuffer[ProvenanceRow[V]])] =
//      rdd.combineByKeyWithClassTag[CompactBuffer[ProvenanceRow[V]]](
//        createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
//
//    val underlyingResult: RDD[(K, ProvenanceGrouping[V])] =
//      bufs.mapValues(buf => new ProvenanceGrouping(buf))
//    new PairProvenanceGroupingRDD(underlyingResult)
//  }
//
//  override def groupByKey(numPartitions: Int): PairProvenanceGroupingRDD[K, V] = {
//    groupByKey(new HashPartitioner(numPartitions))
//  }
//
//  override def groupByKey(): PairProvenanceGroupingRDD[K, V] = {
//    groupByKey(defaultPartitioner)
//  }

//  // This looks like the naive approach, but returns a ProvenanceGrouping
//  override def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, ProvenanceGrouping[V]]  =  {
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: ProvenanceRow[V]) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[ProvenanceRow[V]], v: ProvenanceRow[V]) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[ProvenanceRow[V]], c2: CompactBuffer[ProvenanceRow[V]]) => c1 ++= c2
//    val serializer = if(Provenance.useDedupSerializer) {
//      val baseSerializer = SparkEnv.get.serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
//      new ProvenanceDeduplicationSerializer(baseSerializer, partitioner)
//    } else {
//      null // null is the default argument value used, so it's safe here.
//    }
//
//    val bufs: RDD[(K, CompactBuffer[ProvenanceRow[V]])] =
//      rdd.combineByKeyWithClassTag[CompactBuffer[ProvenanceRow[V]]](
//        createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false,
//        serializer = serializer)
//
//    val underlyingResult: RDD[(K, (ProvenanceGrouping[V], Provenance))] =
//      bufs.mapValues(buf => {
//        val group = new ProvenanceGrouping(buf)
//        // TODO: for correctness, this provenance should be group.combinedProvenance
//        // However, for something such as pagerank, we know it is not required because it is not
//        // used later
//        // Is there a way we can leverage the DAG information or otherwise "look ahead" to
//        // determine what to do here?
//        val groupProvenance = group.combinedProvenance
//
//        (group, groupProvenance)
//      })
//    new PairProvenanceDefaultRDD(underlyingResult)
//  }
//
//  override def groupByKey(numPartitions: Int): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(new HashPartitioner(numPartitions))
//  }
//
//  override def groupByKey(): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(defaultPartitioner)
//  }

  def groupByKey(partitioner: Partitioner): PairProvenanceDefaultRDD[K, Iterable[V]] =  {

    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => ArrayBuffer(v)
    val mergeValue = (buf: ArrayBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => c1 ++= c2
    val bufs: PairProvenanceDefaultRDD[K, ArrayBuffer[V]] = combineByKeyWithClassTag[ArrayBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)

    // Final cast of CompactBuffer -> Iterable for API matching
    bufs.asInstanceOf[PairProvenanceDefaultRDD[K, Iterable[V]]]
  }

  def groupByKey(numPartitions: Int): PairProvenanceDefaultRDD[K, Iterable[V]] = {
    groupByKey(new HashPartitioner(numPartitions))
  }

  def groupByKey(): PairProvenanceDefaultRDD[K, Iterable[V]] = {
    groupByKey(defaultPartitioner)
  }

  /** Join two RDDs while maintaining the key-key lineage. This operation is currently only
    * supported for RDDs that possess the same base input RDD.
    */
  override def join[W](other: PairProvenanceDefaultRDD[K, W],
                       partitioner: Partitioner = defaultPartitioner
             ): PairProvenanceDefaultRDD[K, (V, W)] = {
//    if(rdd.firstSource != other.rdd.firstSource) {
//      println("=====\nSEVERE WARNING: Provenance-based join is currently supported only for RDDs " +
//                "originating from the same " +
//                "input data (e.g. self-join): " + s"\n${rdd.firstSource}\nvs.\n${other.rdd
//                                                                                      .firstSource}\n=====")
//    }
    val result: RDD[(K, ProvenanceRow[(V, W)])] = rdd.cogroup(other.rdd).flatMapValues((pair: (Iterable[(V, Provenance)], Iterable[(W, Provenance)])) =>
           for (thisRow <- pair._1.iterator; otherRow <- pair._2.iterator)
             // TODO: enhance this provenance precision somehow.
             // TODO: would it help to lazy merge if the two provenance objects are equivalent?
             yield ((thisRow._1, otherRow._1), thisRow._2.cloneProvenance().merge(otherRow._2))
                                                                                       )
    new PairProvenanceDefaultRDD(result)
  }

//  override def groupByKey(): PairProvenanceDefaultRDD[K, Iterable[V]] = {
//    val result: RDD[(K, Iterable[(V, Provenance)])] = rdd.groupByKey()
//    new PairProvenanceDefaultRDD(result)
//  }

  override def sortBy[O](f: ((K, V)) => O, ascending: Boolean)
                        (implicit ord: Ordering[O], ctag: ClassTag[O]): PairProvenanceRDD[K, V] = {
    val wrapper = (tuple: (K, (V, Provenance))) => {
      tuple match {
        case (k, (v, _)) => f((k, v))
      }}
    new PairProvenanceDefaultRDD(rdd.sortBy(wrapper, ascending))
  }

  override def foreach(f: ((K, V)) => Unit): Unit = rdd.foreach{case (k, (v, _)) => f((k,v))}

  def sample(withReplacement: Boolean, fraction: Double): PairProvenanceDefaultRDD[K,V]  = {
    new PairProvenanceDefaultRDD(rdd.sample(withReplacement, math.min(fraction, 1.0)))
  }

}

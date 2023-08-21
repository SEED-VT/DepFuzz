package taintedprimitives

import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import provenance.data.InfluenceMarker.InfluenceMarker
import provenance.data._
import provenance.rdd.{CombinerWithInfluence, FlatProvenanceDefaultRDD, ProvenanceRDD, ProvenanceRow}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

/**
  * Created by malig on 5/3/19.
  */
object PackIntIntoLong {
  private final val RIGHT: Long = 0xFFFFFFFFL

  def apply(left: Int, right: Int): Long =
    left.toLong << 32 | right & 0xFFFFFFFFL

  def getLeft(value: Long): Int =
    (value >>> 32).toInt // >>> operator 0-fills from left

  def getRight(value: Long): Int = (value & RIGHT).toInt
}

object Utils {

  def print(s: String): Unit = {
    println("\n" + s)
  }
  
  def measureTimeMillis[T](block: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = block
    val finishTime = System.currentTimeMillis()
    val elapsed = finishTime - startTime
    (result, elapsed)
  }

  private var zipped_input_RDD: RDD[(String, Long)] = null;
  def setInputZip(rdd: RDD[(String, Long)]): RDD[(String, Long)] = {
    zipped_input_RDD = rdd
    rdd
  }

  private val colProvRDD: ListBuffer[RDD[(Array[TaintedString], Provenance)]] = new ListBuffer[RDD[(Array[TaintedString], Provenance)]];
  def setInputZip[T: ClassTag](rdd: RDD[T]): RDD[T] = {
    rdd match {
      case _ : RDD[(Array[TaintedString], Provenance)] => colProvRDD.append(rdd.asInstanceOf[RDD[(Array[TaintedString], Provenance)]])
      case _ => throw new UnsupportedOperationException(s"Can't set prov rdd in utils, type mismatch")
    }
    rdd
  }

  private var createCol: String => Array[String] = null
  def setCreateCol(fn: String => Array[String]) : Unit = {
    createCol = fn
  }


  def retrieveProvenance(rr: RoaringBitmap): RDD[String] = {
    val rdd = zipped_input_RDD
      .filter(s => rr.contains(s._2.asInstanceOf[Int]))
      .map(s => s._1)
    rdd
  }

  def retrieveColProvenance(rr: RoaringBitmap, dataset: Int): RDD[String] = {
    val rdd = colProvRDD(dataset)
      .filter(s => RoaringBitmap.andCardinality(rr, s._2.asInstanceOf[DualRBProvenance].bitmap) > 0)
      .map(s => s._1.map(_.value).mkString(","))
    rdd
  }

  def retrieveRowNumbers(rr: RoaringBitmap, dataset: Int): RDD[Int] = {
    val rdd = colProvRDD(dataset)
      .zipWithIndex
      .filter{case (s, _) => RoaringBitmap.andCardinality(rr, s._2.asInstanceOf[DualRBProvenance].bitmap) > 0}
      .map{case (_, i) => i.toInt}
    rdd
  }

  def retrieveRowNumbersAndRows(rr: RoaringBitmap, dataset: Int): RDD[(String, Int)] = {
    val rdd = colProvRDD(dataset)
      .zipWithIndex
      .filter { case (s, _) => RoaringBitmap.andCardinality(rr, s._2.asInstanceOf[DualRBProvenance].bitmap) > 0 }
      .map { case (s, i) => (s._1.map(_.value).mkString(","), i.toInt) }
    rdd
  }

  def retrieveRowNumbers(prov: DualRBProvenance, dataset: Int): RDD[Int] = {
    retrieveRowNumbers(prov.bitmap, dataset)
  }

  def retrieveColumnsFromBitmap(rb: RoaringBitmap) : List[(Int, Int)] = {
    val checkLimit = 10
    var count = 0
    val iter = rb.iterator()
    val set = new mutable.HashSet[Int]()
    val datasets = new mutable.HashSet[Int]()
    val tup = new mutable.HashSet[(Int, Int)]()
    while (/*count < checkLimit && */iter.hasNext()) {
      val int = iter.next()
      val col = (int & DualRBProvenance.MASK_COL) >>> DualRBProvenance.ROW_BITS
      val ds = (int & DualRBProvenance.MASK_DS) >>> (DualRBProvenance.ROW_BITS + DualRBProvenance.COL_BITS)
      set.add(col)
      datasets.add(ds)
      tup.add((ds, col))
      count+=1
    }
    if (datasets.iterator.hasNext) {
      tup.toList
    } else {
      List()
    }
  }

  def retrieveProvenance(prov: Provenance): RDD[String] = {
    prov match {
      case rp: RoaringBitmapProvenance =>
        retrieveProvenance(rp.bitmap)
      case dp: DualRBProvenance =>
        retrieveColProvenance(dp.bitmap, 0)
      case _: DummyProvenance =>
        retrieveProvenance(new RoaringBitmap())
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported provenance retrieval for object " +
                                                  s"$prov")
    }
  }

  def retrieveProvenance(prov: DualRBProvenance, dataset: Int): RDD[String] = {
    retrieveColProvenance(prov.bitmap, dataset)
  }
  
  def retrieveProvenance(prov: Provenance, base: ProvenanceRDD[String]): RDD[String] = {
    val inputRDD = base.asInstanceOf[FlatProvenanceDefaultRDD[String]].rdd.map(_._1)
    zipped_input_RDD =  inputRDD.zipWithUniqueId()
    retrieveProvenance(prov)
  }

  /** Utility method to extract symbolic provenance from object to construct provenance row, if
    * applicable. This method should not be used if udfAware is disabled! */
  def buildSymbolicProvenanceRow[T](in: T, rowProv: Provenance = DummyProvenance.create()): ProvenanceRow[T] = {
    // Might be worth looking into classtags to see if we can avoid this runtime check altogether
    // and simply define methods beforehand.
    (in, inferProvenance(in, rowProv))
  }
  
  /** Recursively inspects data types and infers provenance. The defaultProv is a fallback used
    * if any non-symbolic or unsupported type is observed. */
  def inferProvenance[Any](in: Any, defaultProv: Provenance = DummyProvenance.create()): Provenance = {
    in match {
      case o: TaintedBase =>
        o.getProvenance()
      case product: Product =>
        product.productIterator.map(x => inferProvenance(x, defaultProv))
                .foldLeft(DummyProvenance.create())(_.merge(_))
      /*case product2: Product2[_,_] => // includes Tuple
        product2 match {
          case (a: TaintedBase, b: TaintedBase) =>
            product2.productIterator.map(x => inferProvenance(x, defaultProv))
                    .foldLeft(DummyProvenance.create())(_.merge(_))
          case _ =>
            // default situation, can't match all symbolic types
            defaultProv
        }
      case product3: Product3[_,_,_] =>
        product3 match {
          case (a: TaintedBase, b: TaintedBase, c: TaintedBase) =>
            //a.getProvenance().merge(b.getProvenance()).merge(c.getProvenance())
            product3.productIterator.map(x => inferProvenance(x, defaultProv))
                    .foldLeft(DummyProvenance.create())(_.merge(_))
        }
        */
      case coll: Traversable[_] =>
        coll.map(x => inferProvenance(x, defaultProv))
                .foldLeft(DummyProvenance.create())(_.merge(_))
      //case coll: Traversable[_] =>
      //  defaultProv
      case _ =>
        defaultProv
    }
  }
  
  /** Use provided function to replace existing provenance with new provenance, recursively.
    * Return the original input object, after updating symbolic provenance.
    */
  def replaceSymProvenance[T](in: T, replaceFn: Provenance => Provenance): T = {
    in match {
      case o: TaintedBase =>
        o.setProvenance(replaceFn(o.getProvenance()))
      case product: Product =>
        product.productIterator.foreach(x => replaceSymProvenance(x, replaceFn))
      case coll: Traversable[_] =>
        coll.foreach(x => replaceSymProvenance(x, replaceFn))
      case _ =>
        // do nothing
    }
    in
  }
  
  def replaceSymProvenance[T](in: T, replacement: Provenance): T =
    replaceSymProvenance(in, _ => replacement)
  
  def simplifySyms[T](in: T): String = {
    in match {
      case o: TaintedAny[_] =>
        o.value.toString
      case product: Product =>
        // toSeq since we can't reuse the iterator
        val temp = product.productIterator.map(simplifySyms).filter(_.nonEmpty).toSeq
        if(temp.size > 1) temp.mkString("(",",",")") else temp.mkString // no start/end
      case coll: Traversable[_] =>
        val temp = coll.map(simplifySyms).filter(_.nonEmpty).toSeq
        if(temp.size > 1) temp.mkString("[",",","]") else temp.mkString // no start/end
      case other =>
        other.toString
    }
  }
  
  def computeOneToOneUDF[T, U](f: T => U,
                               input: ProvenanceRow[T],
                               udfAware: Boolean): ProvenanceRow[U] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    if(udfAware) {
      buildSymbolicProvenanceRow(f(input._1), input._2)
    } else {
      input._1 match {
        case r: TaintedBase =>
          // Note: this is an optimization that must be done *before* calling f on the input.
          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
        case _ =>
      }
      (f(input._1), input._2) // use input provenance and pass it to next operator
    }
//    input._1 match {
//      case r: TaintedBase =>
//        if (!udfAware) {
//          // Note: this is an optimization that must be done *before* calling f on the input.
//          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
//          (out, input._2) // use input provenance and pass it to next operator
//        } else {
//          buildSymbolicProvenanceRow(out, input._2)
//        }
//      case r => {
//        if(!udfAware)
//          (out, input._2)
//        else {
//          buildSymbolicProvenanceRow(out, input._2)
//        }
//      }
//    }
  }

  def combine(r: Long, c: Long, dataset_id: Long): Long = {
    r | (c << DualRBProvenance.ROW_BITS) | (dataset_id << (DualRBProvenance.ROW_BITS + DualRBProvenance.COL_BITS))
  }
  def makeProv(row: Long, t: (String, Int), provCreatorFn: Seq[Long] => Provenance, dataset_id: Long): TaintedString = {
    val (v, col) = t
    val provNum = combine(row, col, dataset_id)
    val prov = provCreatorFn(Seq(provNum))
//    println(s"assigning prov ${provNum.toBinaryString} to col $col")
    TaintedString(v, prov)
  }

  def makeSeq(zipped: Array[(String, Int)], row: Long, dataset_id: Long): Array[Long] = {
    zipped.map(t => {
      val (_, col) = t
      combine(row, col, dataset_id)
    })
  }

  def attachProv[T: ClassTag](record: (String, Long), followup: (Array[TaintedString], Provenance) => T, createCol: String => Array[String], dataset_id: Long): T = {
    val provCreatorFn = Provenance.createFn()
    val split = createCol(record._1)
    val zipped = split.zip({0 to split.length})
//    println(s"zipped: ${zipped.toList}")
    val cols = zipped.map(t => makeProv(record._2, t, provCreatorFn, dataset_id))
    val seq = makeSeq(zipped, record._2, dataset_id)
    followup(cols, provCreatorFn(seq))
  }

  // TODO: Consider utilizing ProvenanceGrouping object??
  def computeOneToManyUDF[T, U](
      f: T => TraversableOnce[U],
      input: ProvenanceRow[T],
      udfAware: Boolean): TraversableOnce[ProvenanceRow[U]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    if(udfAware) {
      f(input._1).map(buildSymbolicProvenanceRow(_, input._2))
    } else {
      input._1 match {
        case r: TaintedBase =>
          // Note: this is an optimization that must be done *before* calling f on the input.
          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
        case _ =>
      }
      f(input._1).map((_, input._2)) // use input provenance and pass it to next operator
    }
    
//    input._1 match {
//      case r: TaintedBase =>
//        if (!udfAware) {
//          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
//          f(input._1).map((_, input._2))
//        } else {
//          f(input._1).map { out =>
//            out match {
//              case o: TaintedBase =>
//                (out, o.getProvenance())
//              case a => (a, input._2)
//            }
//          }
//        }
//      case r =>
//        f(r).map((_, input._2))
//    }
  }

  /** Takes in two provenance rows and returns the one that is selected by the Marker.
    * V here is the source value which has the most inlfuence on the combinerd output
    * It is not the combined output [Gulzar] */
  private def mergeWithInfluence[V](prov: ProvenanceRow[V],
                                 prov_other: ProvenanceRow[V],
                                 infl: InfluenceMarker): ProvenanceRow[V] = {
    infl match {
      case InfluenceMarker.right => prov_other
      case InfluenceMarker.left  => prov
      case InfluenceMarker.both  => (prov._1 , prov._2.merge(prov_other._2))
    }
  }


  /**
  * Combined combiner with a value and rank the provenance based on the influence function if given
  * Also check if UDFAwareProvenance is enabled
  */
  def computeCombinerWithValueUDF[C, V](f: (C, V) => C,
                               combiner: ProvenanceRow[CombinerWithInfluence[C,V]],
                               value: ProvenanceRow[V],
                               udfAware: Boolean,
                               inflFunction: Option[(V, V) => InfluenceMarker] =
                                 None): ProvenanceRow[CombinerWithInfluence[C,V]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    // TODO: optimization on provenance - if the output of f (ie, the combiner) is symbolic, we
    //  can extract its provenance assuming udfAware is true. This is independent of input types.
    // TODO: both input arguments can individually have their provenance disabled if necessary,
    //  rather than simultaneously (e.g. if only one of V/C are symbolic, disable accordingly).
    //  (this is only a performance improvement).
    (value._1, combiner._1._1) match {
      case (v: TaintedBase, c: TaintedBase) =>
        if (!udfAware) {

          v.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          c.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          val combiner_influence  = (combiner._1._2 , combiner._2)
          val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , inflFunction.getOrElse((c: V, v: V) =>
              InfluenceMarker.both)(combiner._1._2, value._1))
          ( (f(combiner._1._1, value._1), infl_value ) ,prov )

        } else {

          val out = f(combiner._1._1, value._1)
          out match {
            case o: TaintedBase =>
              ( ( out, value._1),  o.getProvenance())
            case a =>
              val combiner_influence  = (combiner._1._2 , combiner._2)
              val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , InfluenceMarker.both)
              ( (a, infl_value ) ,prov )
          }
        }

      case r =>
        val combiner_influence  = (combiner._1._2 , combiner._2)
        val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , inflFunction.getOrElse((c: V, v: V) =>
          InfluenceMarker.both)(combiner._1._2, value._1))
        ( (f(combiner._1._1, value._1), infl_value ) , if(udfAware) DummyProvenance.create() else prov )
    }
  }
  

  /**
    * Combined combiner with another combiner and rank the provenance based on the influence function if given
    * otherwise merges the provenance.
    *
    * Also check if UDFAwareProvenance is enabled
    * */
  def computeCombinerWithCombinerUDF[C, V](f: (C, C) => C,
                                        combiner1: ProvenanceRow[CombinerWithInfluence[C,V]],
                                        combiner2: ProvenanceRow[CombinerWithInfluence[C,V]],
                                        udfAware: Boolean,
                                        inflFunction: Option[(V, V) => InfluenceMarker] =
                                        None): ProvenanceRow[CombinerWithInfluence[C,V]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    // TODO: optimization on provenance - if the output of f (ie, the combiner) is symbolic, we
    //  can extract its provenance assuming udfAware is true. This is independent of input types.
    // TODO: both input arguments can individually have their provenance disabled if necessary,
    //  rather than simultaneously (e.g. if only one of the two are symbolic, disable accordingly).
    //  (this is only a performance improvement).
    (combiner1._1._1 , combiner2._1._1) match {
      case (v: TaintedBase, c: TaintedBase) =>
        if (!udfAware) {

          v.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          c.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2), inflFunction.getOrElse((c: V, v: V) =>
            InfluenceMarker.both)(combiner1._1._2, combiner2._1._2))
          ( (f(combiner1._1._1 , combiner2._1._1), infl_value ) ,prov )

        } else {

          val out = f(combiner1._1._1 , combiner2._1._1)
          out match {
            case o: TaintedBase =>
              ( ( out, combiner1._1._2),  o.getProvenance()) // Using a random influence V.
            case a =>
              val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2), InfluenceMarker.both)
              ( (a, infl_value ) ,prov )
          }
        }

      case r =>
        val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2) , inflFunction.getOrElse((c: V, v: V) =>
          InfluenceMarker.both)(combiner1._1._2, combiner2._1._2))
        ( (f(combiner1._1._1 , combiner2._1._1), infl_value ) ,if(udfAware) DummyProvenance.create() else prov  )
    }
  }

  def createCombinerForReduce[V,C](createCombiner: V => C , value: V, prov: Provenance, udfAware:Boolean):
  ProvenanceRow[CombinerWithInfluence[C,V]] = {
    ((createCombiner(value) , value), prov)
  }
  
  
  /** Default application configuration flag to determine whether or not to propogate row-level
    * provenance when symbolic objects are used. */
  private var defaultUDFAwareEnabled = false
  
  /** Application configuration flag to determine whether or not to propogate row-level
    * provenance when symbolic objects are used. Defaults to internal variable if None. */
  def getUDFAwareEnabledValue(opt: Option[Boolean]): Boolean = {
    opt.getOrElse(defaultUDFAwareEnabled)
  }
  
  def setUDFAwareDefaultValue(value: Boolean): Unit = {
    defaultUDFAwareEnabled = value
  }
  
  /** Computes the union of all (non-nested) TaintedBase provenances in the list. */
  def addProvDependency(list: List[Any]): Provenance = {
    val symBases = list.collect({case s: TaintedBase => s})
    if(symBases.isEmpty) DummyProvenance.create() else
        symBases.foldLeft(Provenance.create())((prov,symbase) => prov.merge(symbase.getProvenance()))
  }
  
  // A regular expression to match classes of the internal Spark API's
  // that we want to skip when finding the call site of a method.
  private val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  private val SYMEx_CORE_REGEX = """^main\.taintedprimitives.*""".r

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX
      .findFirstIn(className)
      .isDefined ||
      SYMEx_CORE_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass
  }

  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction)
    : CallSite = {
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    val callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      if (ste != null && ste.getMethodName != null
          && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            if (ste.getFileName != null) {
              firstUserFile = ste.getFileName
              if (ste.getLineNumber >= 0) {
                firstUserLine = ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
        // s"$lastSparkMethod at $firstUserFile:$firstUserLine"
        s"$firstUserFile:$firstUserLine"

      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }
  
  def printWithLimit(arr: Array[_],
                     limitOpt: Option[Int],
                     header: String,
                     lineSep: String = "-" * 50): Unit = {
    limitOpt.foreach(limit => {
      if(limit == Int.MaxValue || arr.length <= limit) println(s"$header (all results)")
      else println(s"$header (capped at $limit printed)")
      
      arr.take(limit).foreach(println)
      println(lineSep)
    })
  }
  
  def runTraceAndPrintStats[OutSchema](out: ProvenanceRDD[OutSchema],
                                       outputTestFn: OutSchema => Boolean,
                                       inputs: ProvenanceRDD[String],
                                       inputFaultFn: String => Boolean,
                                       outputPrintLimit: Option[Int] = Some(10),
                                       debugPrintLimit: Option[Int] = Some(10),
                                       tracePrintLimit: Option[Int] = Some(10),
                                       diffsPrintLimit: Option[Int] = Some(10),
                                       lineSep: String = "-" * 50
                                      ): Array[String] = {
    // standard extractor, i.e. for non-symbolic cases.
    val outputProvenanceExtractor: ProvenanceRow[OutSchema] => Provenance = _._2
    runTraceAndPrintStatsWithProvExtractor(out, outputTestFn, outputProvenanceExtractor, inputs, inputFaultFn, outputPrintLimit,
                          debugPrintLimit, tracePrintLimit, diffsPrintLimit, lineSep)
  }
  
  def runTraceAndPrintStatsWithProvExtractor[OutSchema](out: ProvenanceRDD[OutSchema],
                                       outputTestFn: OutSchema => Boolean,
                                       outputProvExtractor: ProvenanceRow[OutSchema] => Provenance,
                                       inputs: ProvenanceRDD[String],
                                       inputFaultFn: String => Boolean,
                                       outputPrintLimit: Option[Int] = Some(10),
                                       debugPrintLimit: Option[Int] = Some(10),
                                       tracePrintLimit: Option[Int] = Some(10),
                                       diffsPrintLimit: Option[Int] = Some(10),
                                       lineSep: String = "-" * 50
                                    ): Array[String] = {
    
    
    val (outResults, collectTime) = Utils.measureTimeMillis(out.collectWithProvenance())
    val debugTargets = outResults.filter(tuple => outputTestFn(tuple._1))
    val inputCount = inputs.count()
    val outputCount = outResults.length
  
    val debugTargetCount = debugTargets.length
    val combinedProvenance = debugTargets.map(outputProvExtractor).foldLeft(DummyProvenance.create())({
      case (prov, other) => prov.merge(other)
    })
    
    val (traceResults, traceTime) =
      Utils.measureTimeMillis(Utils.retrieveProvenance(combinedProvenance).collect())
    val traceCount = traceResults.length
    val traceCorrectCount = traceResults.count(inputFaultFn)
    
    val trueFaultsRDD: ProvenanceRDD[String] = inputs.filter(inputFaultFn)
    val (trueFaultCount, missingFaults, falseTraces) = if(diffsPrintLimit.isDefined) {
      val trueFaults = trueFaultsRDD.collect()
      val missingFaults = trueFaults.diff(traceResults)
      val falseTraces = traceResults.diff(trueFaults)
      (trueFaults.length.toLong, missingFaults, falseTraces)
    } else (trueFaultsRDD.count(), Array(), Array())
  
    
    val missingFaultCount = trueFaultCount - traceCorrectCount
    val extraTraceCount = traceCount - traceCorrectCount
    
    println(lineSep)
    printWithLimit(outResults, outputPrintLimit, "OUTPUTS", lineSep)
    printWithLimit(outResults.map(Utils.simplifySyms), outputPrintLimit,
                   "OUTPUTS (simplified)", lineSep)
    printWithLimit(debugTargets, debugPrintLimit, "DEBUG TARGETS", lineSep)
    printWithLimit(traceResults, tracePrintLimit, "TRACE RESULTS", lineSep)
    printWithLimit(missingFaults, diffsPrintLimit, "UNTRACED FAULTS", lineSep)
    printWithLimit(falseTraces, diffsPrintLimit, "FALSELY TRACED FAULTS", lineSep)
    
    println(s"Collect time: $collectTime")
    println(s"Input count: $inputCount")
    println(s"Output count: $outputCount")
    println(s"Debug target count: $debugTargetCount")
    println(s"Number of true faults: $trueFaultCount")
    println(s"Trace time: $traceTime")
    println(s"Trace count: $traceCount")
    println(s"Trace count (correct/yield): $traceCorrectCount")
    println(s"Missing Faults: $missingFaultCount")
    println(s"Extra traces: $extraTraceCount")
    println()
    println(s"Precision: ${traceCorrectCount.toDouble / traceCount}")
    println(s"Recall: ${traceCorrectCount.toDouble / trueFaultCount}")
    println(s"Yield: $traceCorrectCount")
    traceResults
  }
  
  def runBaseline[OutSchema](out: RDD[OutSchema],
                             outputPrintLimit: Option[Int] = Some(10),
                             lineSep: String = "-" * 50
                                      ): Array[OutSchema] = {
    val (outResults, collectTime) = Utils.measureTimeMillis(out.collect())
    val outputCount = outResults.length
    
    println(lineSep)
    printWithLimit(outResults, outputPrintLimit, "OUTPUTS", lineSep)
    
    println(s"Collect time: $collectTime")
    println(s"Output count: $outputCount")
    outResults
  }
  
  def runBaselineTest[OutSchema](out: ProvenanceRDD[OutSchema],
                             outputPrintLimit: Option[Int] = Some(10),
                             lineSep: String = "-" * 50
                            ): Array[(OutSchema, Provenance)] = {
    val (outResults, collectTime) = Utils.measureTimeMillis(out.collectWithProvenance())
    val outputCount = outResults.length
    
    println(lineSep)
    printWithLimit(outResults, outputPrintLimit, "OUTPUTS", lineSep)
    printWithLimit(outResults.map(simplifySyms), outputPrintLimit,
                   "OUTPUTS (Provenance removed)", lineSep)
    
    
    println(s"Collect time: $collectTime")
    println(s"Output count: $outputCount")
    outResults
  }
}



/** CallSite represents a place in user code. It can have a short and a long form. */
case class CallSite(shortForm: String, longForm: String)

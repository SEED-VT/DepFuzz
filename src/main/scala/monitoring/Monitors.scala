package monitoring

import fuzzer.NewFuzzer.writeToFile
import fuzzer.{Operator, ProvInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import provenance.data.{DualRBProvenance, Provenance}
import provenance.rdd.{PairProvenanceDefaultRDD, PairProvenanceRDD}
import runners.Config
import taintedprimitives.{TaintedBase, Utils}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object Monitors extends Serializable {

  val cache: mutable.Map[Int, Boolean] = mutable.HashMap()
  val provInfo: ProvInfo = new ProvInfo()
  val minData: mutable.Map[Int, ListBuffer[String]] = new mutable.HashMap()
  val dummyBuffer: ListBuffer[Provenance] = new ListBuffer()


//  def updateMinData(p: ListBuffer[Provenance]): Unit = {
//    p.foreach { pi =>
//      val bitmap = pi.asInstanceOf[DualRBProvenance].bitmap
//      val datasets = Utils.retrieveColumnsFromBitmap(bitmap)
//        .groupBy(_._1)
//        .keys
//
//      datasets.foreach{ ds =>
//        val rows = Utils.retrieveColProvenance(bitmap, ds).take(5)
//        if(!this.minData.contains(ds)) {
//          this.minData.update(ds, ListBuffer())
//        }
//        this.minData.update(ds, this.minData(ds) ++ rows)
//      }
//    }
//  }

  def monitorJoin[K<:TaintedBase:ClassTag,V1,V2](d1: PairProvenanceDefaultRDD[K,V1],
                                                 d2: PairProvenanceDefaultRDD[K,V2],
                                                 id: Int): PairProvenanceRDD[K,(V1,V2)] = {

    val joint = d1.join(d2.map{case (k, v) => (k, (k, v))})
//        val count = joint.count()
//        count match {
//     If the data does not get past the join then test separately
//          case 0 =>
//            val buffer1 = d1
//              .sample(false, 0.5*Config.maxSamples/d1.count())
//              .map {
//              case (k1, _) =>
//                k1.getProvenance()
//            }.collect().to[ListBuffer]
//            val buffer2 = d2
//              .sample(false, 0.5*Config.maxSamples/d2.count())
//              .map {
//              case (k2, _) =>
//                k2.getProvenance()
//            }.collect().to[ListBuffer]
//            buffer1
//              .zip(buffer2)
//              .foreach { case (p1, p2) => this.provInfo.update(id, ListBuffer(p1, p2)) }
//          case _ =>
    joint
      .map { case (k1, (_, (k2, _))) => ListBuffer(k1.getProvenance(), k2.getProvenance()) }
      .take(5)
      .to[ListBuffer]
      .foreach { p =>
        this.provInfo.update(Operator("=="), p, id)
      }
//        }

    println("Join Prov")
    println(provInfo)

    joint.map{
      case (k1, (v1, (k2, v2))) =>
        k1.setProvenance(k1.getProvenance().merge(k2.getProvenance()))
        (k1, (v1, v2))
    }
  }

  def monitorPredicate(bool: Boolean, prov: (List[Any], List[Any]), id: Int): Boolean = {
    if (bool) {
      prov._1.foreach {
        case v: TaintedBase =>
          dummyBuffer.append(v.getProvenance()) // WARNING: Not cluster safe, temporary
          this.provInfo.update(Operator("=="), ListBuffer(v.getProvenance()), id)
        case _ =>
      }
    }

    //TODO: Add rows to min data, currently this is running on workers

    bool
  }

  def monitorGte(bool: Boolean,
                            prov: (List[Any], List[Any]),
                            id: Int,
                            tupleAcc: CollectionAccumulator[(String, ListBuffer[Provenance], Int)]): Boolean = {

    if (!cache.contains(id)) {
      if (tupleAcc != null) {
        if (bool) {
          (prov._1.head, prov._2.head) match {
            case (lhs: TaintedBase, rhs: TaintedBase) => tupleAcc.add((">=", ListBuffer(lhs.getProvenance(), rhs.getProvenance()), id))
            case (lhs: TaintedBase, _) => tupleAcc.add((">=", ListBuffer(lhs.getProvenance()), id))
            case _ =>
          }
        }
      }
      cache(id) = true
    }

    bool
  }




  //  def monitorPredicate(bool: TaintedBoolean, prov: (List[Any], List[Any]), id: Int, currentPathConstraint: SymbolicExpression = SymbolicExpression(new SymbolicTree())): Boolean = {
  //    if (bool) {
  //      prov._1.foreach {
  //        case v: TaintedBase => this.provInfo.update(id, ListBuffer(v.getProvenance()))
  //        case _ =>
  //      }
  //    }
  //
  //    val pc = if(!currentPathConstraint.isEmpty)
  //      currentPathConstraint.and(bool.symbolicExpression)
  //    else
  //      bool.symbolicExpression
  //
  //    println(s"PC for branch $id: $pc")
  //    bool
  //  }


  def monitorGroupByKey[K<:TaintedBase:ClassTag,V:ClassTag](dataset: PairProvenanceDefaultRDD[K,V], id: Int): PairProvenanceDefaultRDD[K, Iterable[V]] = {
    println("monitoring gbk")
    dataset
      .sample(false, Config.percentageProv)
      .map { case (k, _) => ListBuffer(k.getProvenance()) }
      .take(5)
      .to[ListBuffer]
      .foreach { p =>
//        updateMinData(p)
        this.provInfo.update(Operator("=="), p, id)
      }

    println("GBK Prov")
    println(provInfo)

    dataset.groupByKey()
  }

  def monitorReduceByKey[K, V](
                                dataset: PairProvenanceDefaultRDD[K, V])(
                                func: (V, V) => V, id: Int)
  : PairProvenanceRDD[K, V] = {

    dataset
      .sample(false, Config.percentageProv)
      .map {
        case (k: TaintedBase, _) =>
          ListBuffer(k.getProvenance())
        case _ =>
          ListBuffer[Provenance]()
      }
      .take(5)
      .to[ListBuffer]
      .foreach { p =>
        this.provInfo.update(Operator("=="), p, id)
//        updateMinData(p)
      }

    println("RBK Prov")
    println(provInfo)

    dataset.reduceByKey(func)
  }

  def monitorFilter[T](rdd: RDD[T], f: T => Boolean): RDD[T] = {
    rdd.filter(f)
  }

  // called at the end of main function
  def finalizeProvenance(tupleAcc: CollectionAccumulator[(String, ListBuffer[Provenance], Int)]): ProvInfo = {
    println("ORIGINAL")
    println(provInfo)
    val infixops = tupleAcc.value.asScala.toList
    infixops.foreach {
      case (op, prov, id) =>
        print("ADDING INFIX: ")
        println(op, prov, id)
        provInfo.update(Operator(op), prov, id)
    }
    provInfo
  }


  def finalizeProvenance(): ProvInfo = {
    println("WARNING: USING DEPRECATED PROVENANCE CONSOLIDATOR")
    provInfo
  }

}
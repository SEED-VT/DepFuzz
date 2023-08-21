package provenance.data

import org.roaringbitmap.RoaringBitmap
import runners.Config
import taintedprimitives.Utils

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable.ListBuffer
//import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.mutable.ArrayBuffer

class DualRBProvenance(var bitmap: RoaringBitmap) extends DataStructureProvenance(bitmap) {

  override def _cloneProvenance(): DualRBProvenance = new DualRBProvenance(bitmap.clone())
  
  override def _merge(other: Provenance): this.type = {
    other match {
      case rbp: DualRBProvenance =>
        bitmap.or(rbp.bitmap)
        // Optional (but potentially unsafe/incorrect?) operation to pre-emptively free memory
        //rbp.bitmap.clear()
      case other => throw new NotImplementedError(s"Unsupported RoaringBitmap merge provenance " +
                                                    s"type! $other")
    }
    this
  }

  override def getProvenanceAsSeq(): List[Int] = {
    val iter = this.bitmap.iterator()
    var p = ArrayBuffer[Int]()
    while (iter.hasNext()){
      p += iter.next()
    }
    p.toList
  }

  /** Returns number of provenance IDs. */
  override def count: Int = bitmap.getCardinality
  
  /** Returns estimate in serialization size, experimental. */
  override def estimateSize: Long = bitmap.getLongSizeInBytes
  
  private var serializationCount: Int = 0
  private var deserializationCount: Int = 0
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    serializationCount += 1
    //if (serializationCount > 1) println(s"TADA!: I've been serialized $serializationCount times
    // " + s"and have $count items!")
    //println(SizeEstimator.estimate(this))
    out.writeObject(bitmap)
  }
  
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    deserializationCount += 1
    bitmap = in.readObject().asInstanceOf[RoaringBitmap]
  }

  private def parseProv(p: Int): String = {
    val row = p & DualRBProvenance.MASK_ROW
    val col = (p & DualRBProvenance.MASK_COL) >>> DualRBProvenance.ROW_BITS
    val ds = (p & DualRBProvenance.MASK_DS) >>> (DualRBProvenance.ROW_BITS + DualRBProvenance.COL_BITS)
    s"d$ds|r$row|c$col"
  }
  override def toString: String = {
    var count = 0
    val printLimit = 10
    val iter = this.bitmap.iterator()
    val buf = new StringBuilder("[")
    while (count < printLimit && iter.hasNext()) {
      buf ++= parseProv(iter.next())
      if (iter.hasNext) buf += ','
      count += 1
    }
    if (iter.hasNext) buf ++= s" ...(${this.bitmap.getCardinality - printLimit} more)"
    buf += ']'
    s"${this.getClass.getSimpleName}: ${buf.toString()}"
  }
  
  override def containsAll(other: Provenance): Boolean = {
    other match {
      case rbp: DualRBProvenance =>
        bitmap.contains(rbp.bitmap)
      case _: DummyProvenance => true
      case other => throw new NotImplementedError(s"Unsupported RoaringBitmap containsAll check: $other")
    }
  }

  override def convertToTuples: ListBuffer[(Int, Int, Int)] = {
    //    ListBuffer(0,1).flatMap(d => ListBuffer(0,5,6).map(c => (d, c, 0)))
    val datasetColumns = Utils.retrieveColumnsFromBitmap(this.bitmap)
    datasetColumns
      .groupBy(_._1)
      .map { case (datasetID, cols) => (cols, Utils.retrieveRowNumbers(this, datasetID).collect().take(Config.maxSamples)) }
      .flatMap { case (dsCols, rows) => dsCols.flatMap {
        case (ds, col) =>
          rows.map {
            row =>
//              println(ds, col, row)
              (ds, col, row)
          }
        }
      }
      .to[ListBuffer]
  }
}

object DualRBProvenance extends ProvenanceFactory {
  final val MAX_BITS = 32
  final val ROW_BITS = 22
  final val COL_BITS = 8
  final val DS_BITS = MAX_BITS - ROW_BITS - COL_BITS
  final val MASK_DS = Int.MinValue >> DS_BITS
  final val MASK_COL = (Int.MinValue >> (DS_BITS + COL_BITS)) ^ MASK_DS
  final val MASK_ROW = 0xFFFFFFFF ^ (MASK_DS | MASK_COL)
  final val MAX_ROW_VAL = 0x003FFFFF
  final val MAX_COL_VAL = MASK_COL >>> ROW_BITS

  /*
      ahmad:  The provenance for 32 bit roaring bitmap is split like the following:
              2 bits     | 8 bits   | 22 bits
              Dataset #  | Column # | Row #

              The column region is initialized to 0xFF
   */

  def initColProv(i: Int): Int = {
    i | MASK_COL
  }

  override def create(ids: Long*): DualRBProvenance = {
    if (ids.exists(_ > Long.MaxValue))
      throw new UnsupportedOperationException(
        s"At least one offset is greater than the maximum allowed value of ${Int.MaxValue}: $ids")
    val bitmap = new RoaringBitmap()
    bitmap.add(ids.map( x => x.toInt): _*)
    new DualRBProvenance(bitmap)
  }
}
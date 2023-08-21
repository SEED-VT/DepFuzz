package fuzzer

import org.apache.spark.rdd.RDD
import provenance.data.{DualRBProvenance, Provenance}
import runners.Config
import taintedprimitives.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordered.orderingToOrdered
import scala.util.Random

case class Operator(symbol: String, isTransitive: Boolean = true) {

  override def toString: String = symbol
}

case class Region(ds: Int, col: Int, row: Int) {

  def getAsTuple: (Int, Int, Int) = (ds, col, row)
}

case class CoDepTuple(operator: Operator, regions: ListBuffer[Region]) {

  val _1: Operator = operator
  val _2: ListBuffer[Region] = regions

}


//depsInfo: [[(ds, col, row), (ds, col, row)], [(ds, col, row)] .... [(ds, col, row)]]
class ProvInfo(val depsInfo: ListBuffer[CoDepTuple], val fullToMinRowMap: mutable.Map[(Int, Int), Int] = new mutable.HashMap()) extends Serializable {

  val minData: mutable.Map[Int, ListBuffer[String]] = new mutable.HashMap()

//  def update(id: Int, provenances: ListBuffer[Provenance]): Unit = {
//    depsInfo.append(provenances.flatMap(_.convertToTuples))
//  }

  def update(op: Operator, provenances: ListBuffer[Provenance], id: Int): Unit = {
    val newRegions = updateMinData(provenances)
    depsInfo.append(CoDepTuple(op,  newRegions))
  }

  def updateMinData(p: ListBuffer[Provenance]): ListBuffer[Region] = {
    p.foreach { pi =>
      val bitmap = pi.asInstanceOf[DualRBProvenance].bitmap
      val datasets = Utils.retrieveColumnsFromBitmap(bitmap)
        .groupBy(_._1)
        .keys

      datasets.foreach { ds =>
        val rowsAndIds = Utils.retrieveRowNumbersAndRows(bitmap, ds).take(5)

        if (!this.minData.contains(ds)) {
          this.minData.update(ds, ListBuffer())
        }

        val offset = this.minData(ds).length
        this.minData.update(ds, this.minData(ds) ++ rowsAndIds.map(_._1))
        rowsAndIds.zipWithIndex.foreach{
          case ((row, rowid), i) =>
            this.fullToMinRowMap.update((ds, rowid), offset+i)
        }
      }
    }
    p.flatMap(_.convertToTuples).map{case (d, c, r) => Region(d, c, this.fullToMinRowMap((d, r)))}
  }

  def this() = {
    this(ListBuffer())
  }

  def getLocs(): ListBuffer[CoDepTuple] = { depsInfo }

  def updateRowSet(newLocs: Map[(Int, Int), (Int, Int)]): ProvInfo = {
    new ProvInfo(depsInfo.map {
      codeptup =>
        CoDepTuple(codeptup.operator, codeptup.regions.map {
        case Region(ds, col, row) =>
          val Some((newDs, newRow)) = newLocs.get((ds, row))
          Region(newDs, col, newRow)
      })
    }, fullToMinRowMap)
  }

  def getRowLevelProvenance(): List[(Int,Int)] = {
    depsInfo.last.regions.map{case Region(ds, _, row) => (ds, row)}.distinct.toList
  }

  def merge(): ProvInfo = {
    // TODO: This is unimplemented
    new ProvInfo(depsInfo, fullToMinRowMap)
  }

  def append(other: ProvInfo): ProvInfo = {
    new ProvInfo(depsInfo ++ other.depsInfo, fullToMinRowMap)
  }

  def getRandom: ProvInfo = {
    new ProvInfo(ListBuffer(Random.shuffle(depsInfo).head), fullToMinRowMap)
  }

  def getCoDependentRegions: ListBuffer[CoDepTuple] = { depsInfo }


  def _mergeSubsets(buffer: ListBuffer[CoDepTuple]): ListBuffer[CoDepTuple] = {
    buffer.foldLeft(ListBuffer[CoDepTuple]()){
      case (acc, e) =>
        val keep = !buffer.filter(_.regions.length > e.regions.length).exists(s => s.operator.isTransitive && e.regions.toSet.subsetOf(s.regions.toSet))
        if(keep) acc :+ e else acc
    }
  }

  def _mergeOverlapping(buffer: ListBuffer[CoDepTuple]): ListBuffer[CoDepTuple] = {
    buffer.foldLeft(ListBuffer[CoDepTuple]()){
      case (acc, e) =>
        val (merged, ne) = buffer.find(s => s.operator.symbol == "==" && !e.equals(s) && e.regions.toSet.intersect(s.regions.toSet).nonEmpty) match {
          case Some(x) => (true, (acc :+ CoDepTuple(e.operator, e.regions.toSet.union(x.regions.toSet).to[ListBuffer].sortWith((a, b) => a.getAsTuple < b.getAsTuple))).distinct)
          case None => (false, ListBuffer(e))
        }
        if(!merged) acc :+ e else ne
    }
  }

  def _simplify(deps: ListBuffer[CoDepTuple]): ListBuffer[CoDepTuple] = {
    _mergeOverlapping(_mergeSubsets(deps.map{tup => CoDepTuple(tup.operator, tup.regions.distinct)}.distinct))
  }

  def simplify(): ProvInfo = {
    new ProvInfo(_simplify(depsInfo), fullToMinRowMap)
  }

  override def toString: String = {
//    return depsInfo.toString
    depsInfo
      .map{
        deps =>
          val row = deps.regions.map{case Region(ds, row, col) => s"($ds,$row,$col)"}.mkString("<=>")
          s"(${deps.operator}, $row)"
      }.mkString("\n----------------------------\n") ++
      "\nMAPPING:\n" ++
      this.fullToMinRowMap.map{case ((ds, row), nr) =>
        s"d:$ds,r:$row => $nr\n"
      }.mkString("\n")
  }
}

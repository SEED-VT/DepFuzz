package examples.monitored

import fuzzer.ProvInfo

import scala.reflect.runtime.universe._
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import provenance.data.Provenance
import provenance.rdd.ProvenanceRDD.toPairRDD
import taintedprimitives.{TaintedInt, TaintedString}
import taintedprimitives.SymImplicits._

object WebpageSegmentation extends Serializable {
  def main(args: Array[String]): ProvInfo = {
    println(s"webpage WebpageSegmentation args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
    val before_data = args(0)
    val after_data = args(1)
    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
    ctx.setLogLevel("ERROR")
//    Provenance.setProvenanceType("dual")
    val before = ctx.textFileProv(before_data, _.split(','))
    val after = ctx.textFileProv(after_data, _.split(','))
    val boxes_before = toPairRDD[TaintedString, (TaintedString, Vector[TaintedInt])](before.map(r => (r(0)+"*"+r(r.length - 2)+"*"+r.last, (r(0), r.slice(1, r.length-2).map(_.toInt).toVector))))
    val boxes_after = toPairRDD[TaintedString, (TaintedString, Vector[TaintedInt])](after.map(r => (r(0)+"*"+r(r.length - 2)+"*"+r.last, (r(0), r.slice(1, r.length-2).map(_.toInt).toVector))))
    val boxes_after_by_site_ungrouped = after.map(r => (r(0), (r.slice(1, r.length - 2).map(_.toInt).toVector, r(r.length - 2), r.last)))
    val boxes_after_by_site = _root_.monitoring.Monitors.monitorGroupByKey(boxes_after_by_site_ungrouped, 0)

    val pairs = _root_.monitoring.Monitors.monitorJoin(boxes_before, boxes_after, 1)
    val changed = toPairRDD[TaintedString, (Vector[TaintedInt], TaintedString, TaintedString)](pairs.filter({
      case (_, ((_, v1), (_, v2))) => !v1.equals(v2)
    }).map({
      case (k, (_, (url, a))) =>
        val Array(_, cid, ctype) = k.split('*')
        (url, (a, cid, ctype))
    }))
    val inter = _root_.monitoring.Monitors.monitorJoin(changed, boxes_after_by_site, 2)
    inter.map{
      case (url, ((box1, _, _), lst)) =>
        (url, lst.map{
          case (box, _, _) => box
        }.map(intersects(_, box1)))
    }.collect().take(10).foreach(println)
    _root_.monitoring.Monitors.finalizeProvenance()
  }
  def intersects(rect1: IndexedSeq[TaintedInt], rect2: IndexedSeq[TaintedInt]): Option[(TaintedInt, TaintedInt, TaintedInt, TaintedInt)] = {
    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2
    val endpointax = aSWx + aWidth
    val startpointax = aSWx
    val endpointay = aSWy + aHeight
    val startpointay = aSWy
    val endpointbx = bSWx + bWidth
    val startpointbx = bSWx
    val endpointby = bSWy + bHeight
    val startpointby = bSWy
    if (_root_.monitoring.Monitors.monitorPredicate(endpointax < startpointbx && startpointax < startpointbx, (List[Any](endpointax, startpointbx, startpointax, startpointbx), List[Any]()), 0)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicate(endpointbx < startpointax && startpointbx < startpointax, (List[Any](endpointbx, startpointax, startpointbx, startpointax), List[Any]()), 1)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicate(endpointby < startpointay && startpointby < startpointay, (List[Any](endpointby, startpointay, startpointby, startpointay), List[Any]()), 2)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicate(endpointay < startpointby && startpointay < startpointby, (List[Any](endpointay, startpointby, startpointay, startpointby), List[Any]()), 3)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicate(startpointay > endpointby, (List[Any](startpointay, endpointby), List[Any]()), 4)) {
      return None
    }
    var iSWx, iSWy, iWidth, iHeight = new TaintedInt(0)
    if (_root_.monitoring.Monitors.monitorPredicate(startpointax <= startpointbx && endpointbx <= endpointax, (List[Any](startpointax, startpointbx, endpointbx, endpointax), List[Any]()), 5)) {
      iSWx = startpointbx
      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 6)) startpointby else startpointay
      iWidth = bWidth
      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 7)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointbx <= startpointax && endpointax <= endpointbx, (List[Any](startpointbx, startpointax, endpointax, endpointbx), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 8)) {
      iSWx = startpointax
      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 9)) startpointby else startpointay
      iWidth = aWidth
      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 10)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointax >= startpointbx && startpointax <= endpointbx, (List[Any](startpointax, startpointbx, startpointax, endpointbx), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 11)) {
      iSWx = startpointax
      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 12)) startpointay else startpointby
      iWidth = endpointbx - startpointax
      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 13)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointbx >= startpointax && startpointbx <= endpointax, (List[Any](startpointbx, startpointax, startpointbx, endpointax), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 14)) {
      iSWx = startpointbx
      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 15)) startpointay else startpointby
      iWidth = endpointax - startpointbx
      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 16)) endpointby else endpointay
      iHeight = top - iSWy
    }
    else if (_root_.monitoring.Monitors.monitorPredicate(startpointbx > startpointax && startpointbx < endpointax && endpointby <= startpointay, (List[Any](startpointbx, startpointax, startpointbx, endpointax, endpointby, startpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 17)) {
      iSWx = startpointbx
      iSWy = startpointay
      iWidth = bWidth
      iHeight = 0
    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointax > startpointbx && startpointax < endpointbx && endpointay <= startpointby, (List[Any](startpointax, startpointbx, startpointax, endpointbx, endpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax, startpointbx, startpointax, startpointbx, endpointax, endpointby, startpointay)), 18)) {
      iSWx = startpointax
      iSWy = endpointby
      iWidth = aWidth
      iHeight = 0
    }
    Some((iSWx, iSWy, iHeight, iWidth))
  }
}
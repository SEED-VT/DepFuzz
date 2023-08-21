//package transformers.testout
//import org.apache.spark.{ SparkConf, SparkContext }, sparkwrapper.SparkContextWithDP, taintedprimitives._, taintedprimitives.SymImplicits._
//object fullTestCase1 {
//  def main(args: Array[TaintedString]): Unit = {
//    println(s"webpage WebpageSegmentation args ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[6]")
//    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
//    val before_data = args(0)
//    val after_data = args(1)
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val before = ctx.textFileProv(before_data, _.split(','))
//    val after = ctx.textFileProv(after_data, _.split(','))
//    val boxes_before = before.map(r => (s"${r(0)}*${r(r.length - 2)}*${r.last}", (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
//    val boxes_after = after.map(r => (s"${r(0)}*${r(r.length - 2)}*${r.last}", (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
//    val boxes_after_by_site = _root_.monitoring.Monitors.monitorGroupByKey(after.map(r => (r(0), (r.slice(1, r.length - 2).map(_.toInt).toVector, r(r.length - 2), r.last))), 4)
//    val pairs = _root_.monitoring.Monitors.monitorJoin(boxes_before, boxes_after, 5)
//    val changed = pairs.filter({
//      case (_, ((_, v1), (_, v2))) =>
//        !v1.equals(v2)
//    }).map({
//      case (k, (_, (url, a))) =>
//        val Array(_, cid, ctype) = k.split('*')
//        (url, (a, cid, ctype))
//    })
//    val inter = _root_.monitoring.Monitors.monitorJoin(changed, boxes_after_by_site, 7)
//    inter.map({
//      case (url, ((box1, _, _), lst)) =>
//        (url, lst.map({
//          case (box, _, _) => box
//        }).map(intersects(_, box1)))
//    }).collect()
//    _root_.monitoring.Monitors.finalizeProvenance()
//  }
//  def intersects(rect1: IndexedSeq[TaintedInt], rect2: IndexedSeq[TaintedInt]): Option[(TaintedInt, TaintedInt, TaintedInt, TaintedInt)] = {
//    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
//    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2
//    val endpointax = aSWx + aWidth
//    val startpointax = aSWx
//    val endpointay = aSWy + aHeight
//    val startpointay = aSWy
//    val endpointbx = bSWx + bWidth
//    val startpointbx = bSWx
//    val endpointby = bSWy + bHeight
//    val startpointby = bSWy
//    if (_root_.monitoring.Monitors.monitorPredicate(endpointax < startpointbx && startpointax < startpointbx, (List[Any](endpointax, startpointbx, startpointax, startpointbx), List[Any]()), 8)) {
//      return None
//    }
//    if (_root_.monitoring.Monitors.monitorPredicate(endpointbx < startpointax && startpointbx < startpointax, (List[Any](endpointbx, startpointax, startpointbx, startpointax), List[Any]()), 9)) {
//      return None
//    }
//    if (_root_.monitoring.Monitors.monitorPredicate(endpointby < startpointay && startpointby < startpointay, (List[Any](endpointby, startpointay, startpointby, startpointay), List[Any]()), 10)) {
//      return None
//    }
//    if (_root_.monitoring.Monitors.monitorPredicate(endpointay < startpointby && startpointay < startpointby, (List[Any](endpointay, startpointby, startpointay, startpointby), List[Any]()), 11)) {
//      return None
//    }
//    if (_root_.monitoring.Monitors.monitorPredicate(startpointay > endpointby, (List[Any](startpointay, endpointby), List[Any]()), 12)) {
//      return None
//    }
//    var iSWx, iSWy, iWidth, iHeight = 0
//    if (_root_.monitoring.Monitors.monitorPredicate(startpointax <= startpointbx && endpointbx <= endpointax, (List[Any](startpointax, startpointbx, endpointbx, endpointax), List[Any]()), 13)) {
//      iSWx = startpointbx
//      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any]()), 14)) startpointby else startpointay
//      iWidth = bWidth
//      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any]()), 15)) endpointby else endpointay
//      iHeight = top - iSWy
//    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointbx <= startpointax && endpointax <= endpointbx, (List[Any](startpointbx, startpointax, endpointax, endpointbx), List[Any]()), 16)) {
//      iSWx = startpointax
//      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any]()), 17)) startpointby else startpointay
//      iWidth = aWidth
//      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any]()), 18)) endpointby else endpointay
//      iHeight = top - iSWy
//    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointax >= startpointbx && startpointax <= endpointbx, (List[Any](startpointax, startpointbx, startpointax, endpointbx), List[Any]()), 19)) {
//      iSWx = startpointax
//      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any]()), 20)) startpointay else startpointby
//      iWidth = endpointbx - startpointax
//      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any]()), 21)) endpointby else endpointay
//      iHeight = top - iSWy
//    } else if (_root_.monitoring.Monitors.monitorPredicate(startpointbx >= startpointax && startpointbx <= endpointax, (List[Any](startpointbx, startpointax, startpointbx, endpointax), List[Any]()), 22)) {
//      iSWx = startpointbx
//      iSWy = if (_root_.monitoring.Monitors.monitorPredicate(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any]()), 23)) startpointay else startpointby
//      iWidth = endpointax - startpointbx
//      val top = if (_root_.monitoring.Monitors.monitorPredicate(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any]()), 24)) endpointby else endpointay
//      iHeight = top - iSWy
//    }
//    Some((iSWx, iSWy, iHeight, iWidth))
//  }
//}
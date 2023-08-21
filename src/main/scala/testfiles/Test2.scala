//package testfiles
//
//import fuzzer.ProvInfo
//import runners.Config
//
//import java.io.{BufferedWriter, File, FileWriter}
//import scala.collection.mutable.ListBuffer
//import scala.util.Random
//
//object Test2 {
//
//  def duplicateRow(datasets: Array[Seq[String]], loc: (Int, Int)): (Array[Seq[String]], (Int, Int)) = {
//    val (ds, row) = loc
//    (datasets.updated(ds, datasets(ds) :+ datasets(ds)(row)), (ds, datasets(ds).length))
//  }
//
//  def provenanceAwareDupication(inputDatasets: Array[Seq[String]], provInfo: ProvInfo, provInfoRand: ProvInfo, n: Int = 3): (Array[Seq[String]], ProvInfo) = {
//    //(0,0,0)<=>(0,5,0)<=>(0,6,0)<=>(1,0,0)<=>(1,5,0)<=>(1,6,0)
//    val toDuplicate = provInfoRand.getRowLevelProvenance() //(DS, ROW)
//
//    //repeat this n times and flatten the resulting provInfoNew
//    val (duplicatedDatasets, provInfoDuplicated) = (0 until n).foldLeft((inputDatasets, new ProvInfo())){
//      case ((accDatasets, accProvInfo), _) =>
//        val (datasetsWithNewRows, newLocs) = toDuplicate.foldLeft((accDatasets, Map[(Int, Int), (Int, Int)]())) {
//          case ((datasets, newLocs), loc) =>
//            val (newDatasets, newLoc) =  duplicateRow(datasets, loc)
//            (newDatasets, newLocs + (loc -> newLoc))
//        }
//        val provInfoNew = provInfoRand.updateRowSet(newLocs)
//        (datasetsWithNewRows, accProvInfo.append(provInfoNew).merge())
//    }
//
//    (duplicatedDatasets, provInfo.append(provInfoDuplicated))
//  }
//  def main(args: Array[String]): Unit = {
//    val datasets = Array(
//      Seq("http://www.youtube.com,0,0,101,1920,1,header"),
//      Seq("http://www.youtube.com,0,0,100,1920,1,header")
//    )
//
//    val provInfo = new ProvInfo(
//        ListBuffer(
//          ListBuffer(0,1).flatMap(d => ListBuffer(0,5,6).map(c => (d, c, 0)))
//        )
//    )
//
//    val (m, n) = (3,3)
//    val provInfoRand = provInfo.getRandom
//    val (duplicated, newInfo) = (0 until m).foldLeft((datasets, provInfo))
//    {case ((accD, accP), _) => provenanceAwareDupication(accD, accP, provInfoRand, n)}
//
//    duplicated.zipWithIndex.foreach{
//      case (dataset, i) =>
//        println(s"Dataset $i---")
//        dataset.foreach(println)
//        println("______________")
//    }
//
//    println(newInfo)
//
//  }
//}

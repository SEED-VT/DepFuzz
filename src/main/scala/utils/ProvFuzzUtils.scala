package utils


import fuzzer.{InstrumentedProgram, ProvInfo, Schema}

import java.io.{BufferedWriter, File, FileWriter}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, blocking, duration}
import scala.util.Random

object ProvFuzzUtils {
  def RandomSampleDatasets(datasets: Array[String], out_dir: String, sample_size: Double): Array[String] = {
    val data = ReadDatasets(datasets)
    val spds = sample_size/datasets.length
    val sampled_data = data.map(d => SelectRandomRows(d, spds))
    WriteDatasets(sampled_data, out_dir)
  }

  def SelectRandomRows(data: Seq[String], per: Double): Seq[String] = {
    val num_samples = (data.length*per).toInt
    val idxs = (0 until num_samples).map(_ => Random.nextInt(data.length))
    data.zipWithIndex.filter{case (_, i) => idxs.contains(i)}.map(_._1)
  }

  def ReadDatasets(datasets: Array[String]): Array[Seq[String]] = {
    datasets.map(ReadDataset)
  }

  def ReadDataset(dataset: String): Seq[String] = {
    utils.FileUtils.readDataset(dataset)
  }

  def WriteToFile(f: File, d: Seq[String]): String = {
    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(d.mkString("\n"))
    bw.close()
    f.getPath
  }

  def WriteDatasets(datasets: Array[Seq[String]], path: String): Array[String] = {
    val filenames = datasets.indices.map(i => s"$path/dataset_$i")
    val files = filenames.map(new File(_))
    filenames.foreach{ case f =>
      if(new File(f).exists()){
        deleteDir(new File(f))
      }}
    files.foreach(_.mkdirs())
    filenames
      .zip(datasets)
      .map{case (f, d) =>
        WriteToFile(new File(s"$f/part-00000"), d)
        f
      }.toArray
  }

  def Probe(program: InstrumentedProgram): (ProvInfo, Long, Long) = {
    val t_start = System.currentTimeMillis()
    val prov_info = program.main(program.args)
    (prov_info, t_start, System.currentTimeMillis())
  }

  def blockFor[T](f: () => T, millis: Int): T = {
    val v = f()
    Thread.sleep(millis)
    v
  }

  def timeoutWrapper[T](func: () => T, timeout: Int, ret: T): T = {
    try {
      Await.result(Future(blocking(func())), duration.Duration(timeout, "sec"))
    } catch {
      case _ => ret
    }
  }

  def deleteDir(file: File): Unit = {
    val contents = file.listFiles
    if (contents != null) for (f <- contents) {
      deleteDir(f)
    }
    file.delete
  }


}

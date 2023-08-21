package utils

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object FileUtils {

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (!(d.exists && d.isDirectory))
      throw new Exception(s"The directory ${d.getPath} does not exist or is not a directory")

    d.listFiles.filter(_.isFile).map(_.toString).toList
  }

  def readFile(file: String): Seq[String] = {
    val bufferedSource = Source.fromFile(file)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close()
    lines
  }

  def readDataset(path: String): Seq[String] = {
    val file_parts = getListOfFiles(path).filter(path => path.contains("part-"))
    file_parts.foldLeft(Seq[String]())((acc, e) => acc ++ readFile(e))
  }

  def readDatasetPart(path: String, part: Int): Seq[String] = {
    val file_parts = getListOfFiles(path).filter(path => path.contains("part-"))
    readFile(file_parts(part))
  }

  def writeToFile(data: Seq[String], path: String): Unit = {
    val file = new File(path)

    if (!file.getParentFile.exists() && !file.getParentFile.mkdirs())
      throw new Exception(s"Directory ${file.getParentFile} could not be created")

    val bw = new BufferedWriter(new FileWriter(file))
    data.foreach(r => bw.write(s"$r\n"))
    bw.close()
  }

}

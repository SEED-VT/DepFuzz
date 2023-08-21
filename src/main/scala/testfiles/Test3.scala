package testfiles

import fuzzer.ProvInfo
import scoverage.Platform.FileWriter

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.util.Random

object Test3 {
  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt( (max - min) + 1 )
  }

  def randFloatBetween(min: Int, max: Int): Float = {
    (randIntBetween(min*math.pow(10.0, 4).toInt, max*math.pow(10.0, 4).toInt)/math.pow(10.0, 4)).toFloat
  }

  def main(args: Array[String]): Unit = {
    println(randIntBetween(-180, 180))
    println(Array("scheduled", "departed", "completed").apply(Random.nextInt(3)))
  }
}

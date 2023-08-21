package utils

import scala.collection.mutable.ListBuffer
import scala.sys.process.ProcessLogger

class Logger(soutFun: String => Unit = s => (), serrFun: String => Unit = s => ()) extends ProcessLogger{
  val stdout = new ListBuffer[String]()
  val stderr = new ListBuffer[String]()

  override def out(s: => String): Unit = {
    soutFun(s)
    stdout.append(s)
  }

  override def err(s: => String): Unit = {
    serrFun(s)
    stderr.append(s)
  }

  override def buffer[T](f: => T): T = ???
}

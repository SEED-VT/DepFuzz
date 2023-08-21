package utils

import java.io.{InputStream, OutputStream}
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import scala.sys.process.{Process, ProcessIO}

class ProcessCommunicator(val cmd: Seq[String], var releaseBlockIf: String => Boolean = _ => true) {

  val lock = new Semaphore(0)
  final val MAX_BUF = 10000
  var released = false
  val procInput = new java.io.PipedOutputStream()
  val procOutput = new java.io.PipedInputStream()
  val procError = new java.io.PipedInputStream()
  val blockForMillis = 5000
  val proc =  Process(cmd).run(new ProcessIO(writeInput, processOutput, processError))
  println("blocking...")
  lock.acquire()
  println("lock released...")

  private def writeInput(in: OutputStream): Unit = {
    val iStream = new java.io.PipedInputStream(procInput)
    val buf = Array.fill(MAX_BUF)((-1).toByte)
    val nBytes = iStream.read(buf)
    println(s"[INPUT] ${new String(buf.iterator.takeWhile(_>=0).toArray)}")
    in.write(buf, 0, nBytes)
    in.close()
  }

  private def processOutput(out: InputStream): Unit = {
    val oStream = new java.io.PipedOutputStream(procOutput)
    val buf = Array.fill(MAX_BUF)((-1).toByte)
    Iterator.iterate(out.read(buf)) { nBytes =>
      val strOut = new String(buf.iterator.takeWhile(_>=0).toArray)
      print(s"[OUTPUT] ${strOut}")
      oStream.write(buf, 0, nBytes)
      if(this.releaseBlockIf(strOut)) {
        //somehow notify main thread
        println("found output")
        lock.release()
      }
      (0 until MAX_BUF).foreach(buf.update(_, (-1).toByte))
      ProvFuzzUtils.timeoutWrapper({() => out.read(buf)} , 20, -1)
//      out.read(buf)
    }.takeWhile(_>=0).toList
    out.close()
    println("closing out stream...")
  }

  private def processError(out: InputStream): Unit = {
    val oStream = new java.io.PipedOutputStream(procError)
    val buf = Array.fill(MAX_BUF)((-1).toByte)
    Iterator.iterate(out.read(buf)) { nBytes =>
      val strOut = new String(buf.iterator.takeWhile(_>=0).toArray)
      print(s"[ERROR] ${strOut}")
      oStream.write(buf, 0, nBytes)
      (0 until MAX_BUF).foreach(buf.update(_, (-1).toByte))
      out.read(buf)
    }.takeWhile(_>=0).toList
    out.close()
    println("closing out stream...")
  }

  def feed(in: String, releaseBlockIf: String => Boolean = _ => true): Unit = {
    println(s"feeding $in")
    this.releaseBlockIf = releaseBlockIf
    val procI = new java.io.PrintWriter(procInput, true)
    procI.println(in)
    procI.close()
    println("blocking")
    this.lock.acquire()
    println("releasing")
  }


  def kill(): Int = {
    proc.destroy()
    proc.exitValue()
  }
}

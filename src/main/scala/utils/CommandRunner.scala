package utils

import java.io.{PipedInputStream, PipedOutputStream}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException, blocking, duration}
import scala.sys.process.Process

object CommandRunner {
  def StartProcess(cmd: Seq[String], stdin: PipedOutputStream, stdout: PipedInputStream): ProcessCommunicator = {
//    new ProcessCommunicator(cmd)
    null
  }


  def RunSbtInstance(classname: String): Any = {
    null
  }

  def Run(cmd: Seq[String], timeout: Int = 10): (List[String], List[String], Int) = {
    val logger = new Logger()
    try{
      val sandbox = Process(cmd).run(logger)
      val code = try {
        Await.result(Future(blocking(sandbox.exitValue())), duration.Duration(timeout, "sec"))
      } catch {
        case _: TimeoutException =>
          sandbox.destroy()
          sandbox.exitValue()
      }
      (logger.stdout.toList, logger.stderr.toList, code)
    }catch{
      case e =>
        println("Failed to run command: ")
        println(cmd.mkString(" "))
        println("Error Message:")
        println(e.getMessage)
        throw new Exception()
    }

  }
}

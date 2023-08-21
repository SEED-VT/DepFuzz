package utils
import org.apache.hadoop.util.Shell.CommandExecutor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import sys.process._
class Executable {

  var path = ""
  var classpath = ""
  var classname = ""
  def this (path: String) {
    this()
    this.path = path
    try {
      val (sout, _, _) = CommandRunner.Run(Seq("find", "/home/ahmad/.cache"))
      this.classpath = sout.filter(s => s.matches(".+\\.jar$")).mkString(":")
    } catch {
      case _: Exception =>
        throw new Exception()
    }
    this.classname = path.split("classes/")(1).replace(".class", "").replaceAll("/",".")
  }

  def execute(args: String, timeout: Int): ExeOut = {

    println(s"fuzzing ${this.path}")
    println(s"fuzzing ${this.classname}")
    println(s"argment ${args}")
    val cmd = Seq("scala", "-cp", s"${this.classpath}:target/scala-2.11/classes", this.classname, args)
    val (sout, serr, code) = CommandRunner.Run(cmd, timeout)
    val output = new ExeOut(path)
    output.exitCode = code
    sout.foreach(println)
    output
  }
}


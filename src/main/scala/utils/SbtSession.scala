package utils

class SbtSession {
  val (stdin, stdout) = (new java.io.PipedOutputStream(), new java.io.PipedInputStream())

  val cmd = "sbt".split("").toSeq
  val sbt = CommandRunner.StartProcess(cmd, stdin, stdout)
  def compile(): Unit = {
    sbt.feed(s"compile")
  }

  def runMain(classname: String, input: String, timeout: Int): Unit = {
    sbt.feed(s"runMain $classname $input")
  }
}

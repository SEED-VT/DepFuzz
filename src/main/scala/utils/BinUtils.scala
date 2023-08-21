package utils

object BinUtils {

  def LoadBinary(path: String): Executable = {
    new Executable(path)
  }

}



package transformers

import scala.meta._

object TestTransformerClass {

  def main(args: Array[String]): Unit = {

    val tree =  s"hello.world".parse[Term].get
    val tree2 =  s"package hello.world".parse[Source].get
    println(tree.structure)
    println(tree2.structure)
  }
}
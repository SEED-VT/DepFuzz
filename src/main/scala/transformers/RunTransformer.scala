package transformers

object RunTransformer {

  def main(args: Array[String]): Unit = {

    val benchmarkName = if(args.nonEmpty) args(0) else "AgeAnalysis"
    val outPathInstrumented = "src/main/scala/examples/instrumented"
    val outPathFWA = "src/main/scala/examples/fwa"

    val sparkProgramClass = s"examples.benchmarks.$benchmarkName"
    val sparkProgramPath = s"src/main/scala/${sparkProgramClass.split('.').mkString("/")}.scala"

    val instPackage = "examples.instrumented"
    val instProgramPath = s"$outPathInstrumented/$benchmarkName.scala"
    val fwaPackage = "examples.fwa"
    val fwaProgramPath = s"$outPathFWA/$benchmarkName.scala"

    val transformer = new SparkProgramTransformer(sparkProgramPath)

    transformer
      .changePackageTo(instPackage)
      .enableTaintProp()
      .attachMonitors()
      .writeTo(instProgramPath)

    transformer
      .changePackageTo(fwaPackage)
      .replaceImports(
        Map(
          "org.apache.spark.SparkConf" -> "abstraction.SparkConf",
          "org.apache.spark.SparkContext" -> "abstraction.SparkContext"
        )
      )
      .writeTo(fwaProgramPath)
  }
}

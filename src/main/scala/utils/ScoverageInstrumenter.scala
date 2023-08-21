package utils

import utils.CompilerUtils.CompileWithScoverage

object ScoverageInstrumenter {

  // Instrument one subject program
  // Sample invocation: program PATH_TO_SRC PATH_TO_OUT
  //      PATH_TO_SRC: Path to the .scala file that needs to be compiled e.g. src/main/scala/examples/fuzzable/FlightDistance.scala
  //      PATH_TO_OUT: Where the measurement files should be collected e.g. target/jazzer-output/FlightDistance/measurements
  //                   Note: This path is inside the jazzer DOCKER container NOT the host machine since the instrumented program is
  //                         running inside the container. You will need to mount a shared volume to the docker container, this path
  //                         should be the docker container's path to the shared volume.

  def main(args: Array[String]): Unit = {
    val classpath = args(0)
    val outDir = args(1)
    CompileWithScoverage(classpath, outDir)
  }

}

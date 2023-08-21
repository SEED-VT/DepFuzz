name := "DepFuzz"
version := "1.0"
scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.scalameta" %% "scalameta" % "4.2.3",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.roaringbitmap" % "RoaringBitmap" % "0.8.11",
  "org.scoverage" %% "scalac-scoverage-plugin" % "1.4.1",
  "org.scoverage" %% "scalac-scoverage-runtime" % "1.4.1",
  "org.scala-lang" % "scala-compiler" % "2.12.2",
  "org.scala-lang" % "scala-reflect" % "2.12.2",
//  "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
  "com.code-intelligence" % "jazzer-api" % "0.11.0"
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}



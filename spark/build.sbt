name := "marketing_analyzer"

version := "0.1"

scalaVersion := "2.12.8"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.17.1",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll
)
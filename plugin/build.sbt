// Set base Scala versions
ThisBuild / scalaVersion := "2.12.18"
crossScalaVersions := Seq("2.12.18", "2.13.13")

// Custom output directory based on scala version
Compile / packageBin / artifactPath := {
  val scalaVer = scalaVersion.value
  file(s"../papaya/plugin_jars/spark-3.5-scala-${scalaVer}.jar")
}

lazy val root = (project in file("."))
  .settings(
    name := "papaya-plugin",
    version := "0.1.0",
    organization := "com.datafruit",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided"
    )
  )

lazy val commonSettings = Seq(
  organization := "com.google.cloud.dataproc",
  version := "1.0.0"
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    scalaVersion := "2.12.18",
    crossScalaVersions := Seq("2.12.18", "2.13.11"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11"
    ),
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.json4s.**" -> "managed_spark_cell_monitoring.shaded.org.json4s.@1").inAll,
      ShadeRule.rename("com.fasterxml.jackson.**" -> "managed_spark_cell_monitoring.shaded.com.fasterxml.jackson.@1").inAll
    )
  )

lazy val spark3 = (project in file("spark-3"))
  .dependsOn(core)
  .settings(
    commonSettings,
    scalaVersion := "2.12.18",
    crossScalaVersions := Seq("2.12.18"),
    name := "managed-spark-cell-monitoring-spark3",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11"
    ),
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.json4s.**" -> "managed_spark_cell_monitoring.shaded.org.json4s.@1").inAll,
      ShadeRule.rename("com.fasterxml.jackson.**" -> "managed_spark_cell_monitoring.shaded.com.fasterxml.jackson.@1").inAll
    )
  )

lazy val spark4 = (project in file("spark-4"))
  .dependsOn(core)
  .settings(
    commonSettings,
    scalaVersion := "2.13.11",
    crossScalaVersions := Seq("2.13.11"),
    name := "managed-spark-cell-monitoring-spark4",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "4.0.0-preview1" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11"
    ),
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.json4s.**" -> "managed_spark_cell_monitoring.shaded.org.json4s.@1").inAll,
      ShadeRule.rename("com.fasterxml.jackson.**" -> "managed_spark_cell_monitoring.shaded.com.fasterxml.jackson.@1").inAll
    )
  )

ThisBuild / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / libraryDependencies += "org.mockito" %% "mockito-scala" % "1.17.12" % Test

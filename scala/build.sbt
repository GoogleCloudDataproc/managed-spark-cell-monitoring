// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

lazy val sharedShadeRules = Seq(
  ShadeRule.rename("org.json4s.**" -> "managed_spark_cell_monitoring.shaded.org.json4s.@1").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "managed_spark_cell_monitoring.shaded.com.fasterxml.jackson.@1").inAll
)

lazy val commonSettings = Seq(
  organization := "com.google.cloud.dataproc",
  version := "0.1.0",
  Compile / unmanagedSourceDirectories += (ThisBuild / baseDirectory).value / "core" / "src" / "main" / "scala",
  Test / unmanagedSourceDirectories += (ThisBuild / baseDirectory).value / "core" / "src" / "test" / "scala"
)

lazy val spark3 = (project in file("spark-3"))
  .settings(
    commonSettings,
    name := "managed-spark-cell-monitoring-spark3",
    scalaVersion := "2.12.18",
    crossScalaVersions := Seq("2.12.18"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11"
    ),
    assembly / assemblyShadeRules := sharedShadeRules
  )

lazy val spark4 = (project in file("spark-4"))
  .settings(
    commonSettings,
    name := "managed-spark-cell-monitoring-spark4",
    scalaVersion := "2.13.11",
    crossScalaVersions := Seq("2.13.11"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.spark" %% "spark-core" % "4.0.0-preview1" % "provided",
      "org.json4s" %% "json4s-jackson" % "4.0.7"
    ),
    assembly / assemblyShadeRules := sharedShadeRules
  )

lazy val root = (project in file("."))
  .aggregate(spark3, spark4)
  .settings(
    publish / skip := true,
    assembly / skip := true
  )

ThisBuild / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / libraryDependencies += "org.mockito" %% "mockito-scala" % "1.17.12" % Test

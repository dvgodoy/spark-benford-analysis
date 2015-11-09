// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

organization := "com.dvgodoy"

name := "spark-benford-analysis"

// Don't forget to set the version
version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.6"

sparkVersion := "1.4.1"

spName := "dvgodoy/spark-benford-analysis"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies  ++= Seq(
  // other dependencies here
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalatestplus" %% "play" % "1.4.0-M3" % "test",
  "org.scalanlp" %% "breeze" % "0.11.2",
  // native libraries are not included by default. add this if you want them (as of 0.7)
  // native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  // the visualization library is distributed separately as well.
  // It depends on LGPL code.
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases"

resolvers += "Maven" at "http://repo1.maven.org/maven2"

spShortDescription := "Benford Analysis for Apache Spark package"

spDescription :=
  """Benford Analysis for Apache Spark package.
  """.stripMargin

//libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.10" % "0.1.3"
//libraryDependencies += "holdenk" % "spark-testing-base" % "1.4.1_0.1.1" % "test"
//libraryDependencies += "brkyvz" % "lazy-linalg" % "0.1.0"

//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

//resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
//  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
//)

//credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

// Add Spark components this package depends on, e.g, "mllib", ....
// sparkComponents ++= Seq("sql", "mllib")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

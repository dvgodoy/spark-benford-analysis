// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

organization := "com.dvgodoy"

name := "spark-benford-analysis"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.6"

sparkVersion := "1.4.1"

spName := "dvgodoy/spark-benford-analysis"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "com.typesafe.play" %% "play-json" % "2.4.2",
  "com.typesafe.play" %% "play-functional" % "2.4.2",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalatestplus" %% "play" % "1.4.0-M3" % "test"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

resolvers += "Akka Repository" at "http://repo.akka.io/releases"

resolvers += "Maven" at "http://repo1.maven.org/maven2"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

spShortDescription := "Benford Analysis for Apache Spark package"

spDescription :=
  """Benford Analysis for Apache Spark package
  """.stripMargin

//credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

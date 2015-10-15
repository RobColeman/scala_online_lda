organization := "rob"

name := "online_lda"

version := "1.0"

scalaVersion := "2.10.4"


resolvers ++= Seq(
  "Typesafe repository snapshots"    at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases"     at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging"
)

val sparkVersion = "1.4.1"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"          % "2.2.4"       % "test",
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.mongodb" %% "casbah" % "2.7.4",
  "org.apache.commons" % "commons-math3"      % "3.3",
  "org.apache.spark" %% "spark-core"          % sparkVersion,
  "org.apache.spark" %% "spark-streaming"     % sparkVersion,
  "org.apache.spark" %% "spark-sql"           % sparkVersion,
  "org.apache.spark" %% "spark-mllib"         % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.10" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kinesis-asl_2.10" % sparkVersion
)
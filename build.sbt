name := "cart"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.2"

libraryDependencies ++= Seq(
  // saprk core and spark sql
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
    // logging
    "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)


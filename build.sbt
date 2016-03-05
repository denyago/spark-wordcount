name := """minimal-scala"""

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

resolvers += "central" at "http://repo1.maven.org/maven2/"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.6.0"

fork in run := true
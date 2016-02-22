name := "Missed Connections Analysis"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.2"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
//lazy val root = (project in file("analysis")).
//    settings(
//        name := "Missed Connections Analysis",
//        libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0",
//        libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
//    )

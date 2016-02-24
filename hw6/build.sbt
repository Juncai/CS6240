name := "Missed Connections Analysis"

version := "1.0"

scalaVersion := "2.10.5"

assemblyJarName in assembly := "Job.jar"

mainClass in assembly := Some("analysis.MissedConnections")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.2"

libraryDependencies += "org.joda" % "joda-convert" % "1.8"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}


//lazy val root = (project in file("analysis")).
//    settings(
//        name := "Missed Connections Analysis",
//        libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0",
//        libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
//    )


lazy val root = (project in file(".")).
    settings(
        name := "Shortest Path",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

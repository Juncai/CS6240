
lazy val root = (project in file(".")).
    settings(
        name := "Word Count",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

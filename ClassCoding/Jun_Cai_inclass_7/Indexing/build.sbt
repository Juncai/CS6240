
lazy val root = (project in file(".")).
    settings(
        name := "Indexing the batting dataset",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

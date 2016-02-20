
lazy val root = (project in file(".")).
    settings(
        name := "Most HR per Dollar",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

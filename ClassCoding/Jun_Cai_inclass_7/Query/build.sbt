
lazy val root = (project in file(".")).
    settings(
        name := "Querying all the teams given player played for, ordered by year",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

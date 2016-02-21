
lazy val root = (project in file(".")).
    settings(
        name := "Best Batting Average",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )

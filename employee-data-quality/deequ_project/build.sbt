name := "employee-data-quality-deequ"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql" % "3.3.4",
  "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2"
)

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

// Exclude signed jars that cause issues
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter {_.data.getName == "slf4j-log4j12-1.7.30.jar"}
}
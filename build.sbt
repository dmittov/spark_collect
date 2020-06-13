name := "spark_collect"

organization := "io.github.dmittov"

version := "0.2"

scalaVersion := "2.11.12"

// EMR-5.30.0
// https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-5x.html
val sparkVersion = "2.4.5"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided" withSources(),
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test"
)

test in assembly := {}
parallelExecution in Test := false
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


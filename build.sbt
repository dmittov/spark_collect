name := "spark_collect"

organization := "io.github.dmittov"

version := "0.1"

scalaVersion := "2.11.12"

// EMR-5.23.0
// https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-5x.html
val sparkVersion = "2.3.0"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided" withSources(),
  "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test"
)

test in assembly := {}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


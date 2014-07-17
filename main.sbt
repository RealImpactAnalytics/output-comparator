name := "Results test"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"

testOptions in Test += Tests.Argument("-oD")

testOptions in Test += Tests.Argument("-u", "target/test-reports/")

resolvers += "Akka Repository" at "http://repo.akka.io/release/"

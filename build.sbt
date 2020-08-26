
name := "scalaJosh"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.google.cloud" % "google-cloud-bigquery" % "1.108.1" exclude ("com.google.guava#guava", "28.2-android") exclude ("com.google.errorprone","2.3.4")
// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.10.2"
libraryDependencies += "io.circe" %% "circe-parser" % "0.9.3"
// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.3"
libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.101.0"
//libraryDependencies += "com.typesafe.play" % "play-slick" % "3.0.3"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.google.cloud" % "google-cloud-dlp" % "0.106.0-beta"
libraryDependencies += "com.google.apis" % "google-api-services-iam" % "v1-rev247-1.23.0"


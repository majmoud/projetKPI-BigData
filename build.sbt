name := "TesteJDK"

version := "0.1"

scalaVersion := "2.12.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// Create and write into a CSV file
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
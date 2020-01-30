name := "Proyecto_Integrador"

version := "0.1"

scalaVersion := "2.13.1"

// Core library, included automatically if any other module is imported.
libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.6.0"

// Java 8 date and time instances.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-java8" % "0.6.0"

// Provides scalaz type class instances for kantan.csv, and vice versa.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-scalaz" % "0.6.0"

// Provides cats type class instances for kantan.csv, and vice versa.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-cats" % "0.6.0"

// Automatic type class instances derivation.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-generic" % "0.6.0"

// Provides instances for refined types.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-refined" % "0.6.0"

// Provides instances for enumeratum types.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-enumeratum" % "0.6.0"

// Provides instances for libra types.
libraryDependencies += "com.nrinaudo" %% "kantan.csv-libra" % "0.6.0"

// https://mvnrepository.com/artifact/com.lihaoyi/upickle
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.9.0"
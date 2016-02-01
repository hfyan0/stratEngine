import AssemblyKeys._ 


name := "Scalable Strategy Engine"

// orgnization name (e.g., the package name of the project)
organization := "org.strategyengine"

version := "1.0-SNAPSHOT"

description := "Scalable Strategy Engine"

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := true

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"                                                                                                                                                                         

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test->default",
  "org.mockito" % "mockito-core" % "1.9.5",
  "joda-time" % "joda-time" % "2.9.1"
)

javaOptions in run += "-Djava.library.path=/usr/lib/x86_64-linux-gnu/jni/"

assemblySettings                                    

packSettings

enablePlugins(JavaAppPackaging, UniversalPlugin)

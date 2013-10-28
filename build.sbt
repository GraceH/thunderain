import AssemblyKeys._

name := "thunderain"

version := "0.1.0"

scalaVersion := "2.9.3"

retrieveManaged := true

assemblySettings

unmanagedJars in Compile <++= baseDirectory map { base =>
  val hiveFile = file(System.getProperty("HIVE_HOME")) / "lib"
  val baseDirectories = (base / "lib") +++ (hiveFile)
  val customJars = (baseDirectories ** "*.jar")
  // Hive uses an old version of guava that doesn't have what we want.
  customJars.classpath.filter(!_.toString.contains("guava"))
}

libraryDependencies += 
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating-SNAPSHOT" excludeAll( ExclusionRule(organization = "asm"))

libraryDependencies += 
  "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating-SNAPSHOT" excludeAll( ExclusionRule(organization = "asm"))

libraryDependencies += 
  "edu.berkeley.cs.amplab" %% "shark" % "0.9.0-SNAPSHOT" excludeAll( ExclusionRule(organization = "asm"))

libraryDependencies += "org.tachyonproject" % "tachyon" % "0.3.0-SNAPSHOT"

// for test only
libraryDependencies += "org.tachyonproject" % "tachyon-tests" % "0.3.0-SNAPSHOT"

// for test only and must use 1.9.1 to compitable with spark's scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "org.mongodb" %% "casbah" % "2.6.3"

resolvers ++= Seq(
   "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "MongoDB Repository" at "http://repo.gradle.org/gradle/repo"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
      val strategy = mergeStrategy(entry)
      if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
      else strategy
    }
  }
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {x => (x.data.getName.contains("spark")
    || x.data.getName.contains("shark")
    || x.data.getName.contains("tachyon"))}
}



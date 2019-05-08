ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.11.12"

name := "Spark-Seed"

val sparkVersion = "2.4.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies.map(_ % "provided")
libraryDependencies ++= Seq("com.typesafe" % "config" % "1.3.4")

lazy val commonSettings = Seq(
  scalaVersion := "2.11.12",
  resolvers += Resolver.mavenLocal,
  resolvers += Resolver.typesafeRepo("releases")
)

commonSettings

outputStrategy := Some(StdoutOutput)

run in Compile := Defaults.runTask(
  fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile, run)
)

assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_2.11-${sparkVersion}_${version.value}.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//assemblyShadeRules in assembly := Seq(
//  ShadeRule
//    .rename(
//      "com.github.mrpowers.spark.daria.**" -> "shadedSparkDariaForSparkPika.@1"
//    )
//    .inAll
//)

//update your Intellij configuration to use `mainRunner` when running from Intellij (not the default)
lazy val mainRunner = project
  .in(file("mainRunner"))
  .dependsOn(RootProject(file(".")))
  .settings(
    commonSettings,
    libraryDependencies ++= sparkDependencies.map(_ % "compile"),
    assembly := new File(""),
    publish := {},
    publishLocal := {}
  )

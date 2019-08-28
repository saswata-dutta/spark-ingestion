ThisBuild / organization := "com.saswata"

ThisBuild / name := "spark-seed"

ThisBuild / scalaVersion := "2.11.12"

ThisBuild / scapegoatVersion := "1.3.8"

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.typesafeRepo("releases"),
  "Artima Maven Repository" at "https://repo.artima.com/releases"
)

lazy val sparkVersion = "2.4.3"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

lazy val awsDependencies =
  Seq(
    "com.amazonaws" % "aws-java-sdk-ssm" % "1.11.562",
    "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4.2"
  )

libraryDependencies ++= (sparkDependencies ++ awsDependencies).map(_ % "provided")

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "mysql" % "mysql-connector-java" % "8.0.16",
  "com.amazonaws" % "aws-java-sdk-ssm" % "1.11.562",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

outputStrategy := Some(StdoutOutput)

run in Compile := Defaults.runTask(
  fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile, run)
)

assembly / assemblyJarName := s"${(ThisBuild / name).value}_${scalaVersion.value}_${sparkVersion}_${version.value}.jar"
assembly / assemblyOption := (assemblyOption in assembly).value
  .copy(includeScala = false)

// update Intellij configuration to use `mainRunner` when running from Intellij (not the default)
lazy val mainRunner = project
  .in(file("mainRunner"))
  .dependsOn(RootProject(file(".")))
  .disablePlugins(AssemblyPlugin)
  .settings(
    libraryDependencies ++= (sparkDependencies ++ awsDependencies).map(_ % "compile"),
    assembly := new File(""),
    publish := {},
    publishLocal := {}
  )

scalacOptions ++= Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfuture", // Turn on future language features.
  "-Xlint:_", // Turn on scala linting
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)

wartremoverWarnings in (Compile, compile) ++= Warts.unsafe
wartremoverWarnings in (Compile, compile) ++= Seq(
  ContribWart.Apply,
  ContribWart.ExposedTuples,
  ContribWart.MissingOverride,
  ContribWart.NoNeedForMonad,
  ContribWart.OldTime,
  ContribWart.SealedCaseClass,
  ContribWart.SomeApply,
  ContribWart.SymbolicName,
  ContribWart.UnintendedLaziness,
  ContribWart.UnsafeInheritance
)

ThisBuild / version := "0.1.0"

name := "Spark-Seed"

val sparkVersion = "2.4.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies.map(_ % "provided")
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"
)
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.12",
  resolvers ++= Seq(
      Resolver.mavenLocal,
      Resolver.typesafeRepo("releases"),
      "Artima Maven Repository" at "https://repo.artima.com/releases"
    )
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

scalacOptions ++= Seq(
  //  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
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
  // "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  // "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
  // "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
  // "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  // "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  // "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  // "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  // "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  // "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  // "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  // "-Xlint:option-implicit", // Option.apply used implicit view.
  // "-Xlint:package-object-classes", // Class or object defined in package object.
  // "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  // "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  // "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  // "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  // "-Xlint:unsound-match", // Pattern match may not be typesafe.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  // "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  // "-Ywarn-unused:_", // Warn if any is unused.
  // "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  // "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  // "-Ywarn-unused:locals", // Warn if a local definition is unused.
  // "-Ywarn-unused:params", // Warn if a value parameter is unused.
  // "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  // "-Ywarn-unused:privates", // Warn if a private member is unused.
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)
scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")

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

scapegoatVersion in ThisBuild := "1.3.8"

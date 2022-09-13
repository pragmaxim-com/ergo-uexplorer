import Dependencies._

Global / cancelable := true // Allow cancellation of forked task without killing SBT

lazy val commonSettings = Seq(
  scalaVersion := "2.12.15",
  organization := "org.ergoplatform",
  version := "0.0.1",
  resolvers ++= Resolver.sonatypeOssRepos("public") ++ Resolver.sonatypeOssRepos("snapshots"),
  scalacOptions += "-P:splain:implicits:true",
  ThisBuild / evictionErrorLevel := Level.Info
)

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(indexer)

lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .settings(commonSettings)
    .enablePlugins(JavaAppPackaging)
    .settings(
      assembly / assemblyJarName := "chain-indexer.jar",
      assembly / assemblyMergeStrategy := {
        case "logback.xml" => MergeStrategy.first
        case other if other.contains("module-info.class") => MergeStrategy.discard
        case other if other.contains(".proto") => MergeStrategy.first
        case other if other.contains("io.netty.versions") => MergeStrategy.first
        case other => (assembly / assemblyMergeStrategy).value(other)
      },
      Universal / mappings ++= (baseDirectory.value / "bin" * "*" get) map(x => x -> ("bin/" + x.getName)),
      Universal / mappings ++= (baseDirectory.value / "conf" * "*" get) map(x => x -> ("conf/" + x.getName)),
      Universal / mappings := {
        val universalMappings = (Universal / mappings).value
        val fatJar = (Compile / assembly).value
        val filtered = universalMappings filter {
          case (file, name) => !name.endsWith(".jar")
        }
        filtered :+ (fatJar -> ("lib/" + fatJar.getName))
      },
      scriptClasspath := Seq((assembly / assemblyJarName).value),
      Universal / javaOptions ++= Seq(
        "-J-Xmx512m",
        "-J-Xms256m",
        "-J-Xss40m",
      )
    )
    .settings(
      libraryDependencies ++= akka ++ sttp ++ scyllaDb ++ monocle ++ logging ++ Seq(
        explorerGrabber, scalaTest, scalaCheck, diffx,
        compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
        compilerPlugin("io.tryp" % "splain" % "0.5.8" cross CrossVersion.patch)
      ),
    )
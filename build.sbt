import Dependencies._

Global / cancelable := true // Allow cancellation of forked task without killing SBT

lazy val commonSettings = Seq(
  organization := "org.ergoplatform",
  version := "0.0.1",
  resolvers ++= Resolver.sonatypeOssRepos("public") ++ Resolver.sonatypeOssRepos("snapshots"),
  ThisBuild / evictionErrorLevel := Level.Info,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:higherKinds",
    "-language:existentials",
    "-unchecked",
    "-Xfatal-warnings"
  )
)

def assemblySettings(moduleName: String) = Seq(
  assembly / assemblyJarName := s"$moduleName.jar",
  assembly / assemblyMergeStrategy := {
    case "logback.xml" => MergeStrategy.first
    case other if other.contains("module-info.class") => MergeStrategy.discard
    case other if other.contains(".proto") => MergeStrategy.first
    case other if other.contains("io.netty.versions") => MergeStrategy.first
    case other => (assembly / assemblyMergeStrategy).value(other)
  },
  Universal / mappings ++= (baseDirectory.value / ".." / ".." / "docker" / s"$moduleName.conf" get) map (x => x -> ("conf/" + x.getName)),
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

lazy val root = (project in file("."))
  .settings(
    name := "ergo-uexplorer"
  ).aggregate(core, indexer, graphql)


lazy val core =
  Utils.mkModule("explorer-core", "explorer-core")
    .enablePlugins(JavaAppPackaging)
    .settings(scalaVersion := "2.13.9")
    .settings(commonSettings)
    .settings(scalacOptions ++= Seq("-Ymacro-annotations", "-language:implicitConversions", "-Ypatmat-exhaust-depth", "off"))
    .settings(
      libraryDependencies ++= cats ++ circe ++ refined ++ enumeratums ++ Seq(newtype)
    )

lazy val indexer =
  Utils.mkModule("chain-indexer", "chain-indexer")
    .enablePlugins(JavaAppPackaging)
    .settings(scalaVersion := "2.13.9")
    .settings(commonSettings)
    .settings(assemblySettings("chain-indexer"))
    .settings(scalacOptions ++= Seq("-Ymacro-annotations", "-language:implicitConversions"))
    .settings(
      libraryDependencies ++= akka ++ sttp ++ cassandraDb ++ monocle ++ enumeratums ++ refined ++ logging ++ Seq(
        ergoWallet, pureConfig, scalaTest, scalaCheck, diffx
      )
    ).dependsOn(core)

lazy val graphql =
  Utils.mkModule("graphql-gateway", "graphql-gateway")
    .enablePlugins(JavaAppPackaging)
    .settings(scalaVersion := "3.2.0")
    .settings(commonSettings)
    .settings(scalacOptions ++= Seq("-explain-types", "-Ykind-projector"))
    .settings(assemblySettings("graphql-gateway"))
    .settings(libraryDependencies ++= caliban ++ cql4s ++ logging)

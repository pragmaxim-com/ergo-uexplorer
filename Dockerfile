FROM sbtscala/scala-sbt:eclipse-temurin-11.0.16_1.7.2_3.2.0 as builder
WORKDIR /mnt
COPY build.sbt ./
COPY project/ project/
COPY modules/chain-indexer/ modules/chain-indexer/
RUN sbt stage

FROM eclipse-temurin:11-jre-jammy
WORKDIR /uexplorer/chain-indexer
COPY --from=builder /mnt/modules/chain-indexer/target/universal/stage/ /uexplorer/chain-indexer
ENTRYPOINT ["/uexplorer/chain-indexer/bin/chain-indexer"]

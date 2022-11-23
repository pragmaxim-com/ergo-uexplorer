FROM mozilla/sbt:11.0.8_1.3.13 as builder
WORKDIR /mnt
COPY build.sbt ./
COPY project/ project/
COPY modules/chain-indexer/ modules/chain-indexer/
RUN sbt stage

FROM openjdk:11-jre-slim
WORKDIR /uexplorer/chain-indexer
COPY --from=builder /mnt/modules/chain-indexer/target/universal/stage/ /uexplorer/chain-indexer
ENTRYPOINT ["/uexplorer/chain-indexer/bin/chain-indexer"]

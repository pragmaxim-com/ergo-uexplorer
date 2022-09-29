FROM mozilla/sbt:11.0.8_1.3.13 as builder
WORKDIR /mnt
RUN rm -rf ./**/target/
COPY build.sbt ./
COPY project/ project/
COPY modules/chain-indexer/ modules/chain-indexer/
COPY explorer-backend.tar.gz ./
RUN mkdir -p /root/.ivy2/local && \
        tar xf explorer-backend.tar.gz -C /root/.ivy2/local && \
        rm explorer-backend.tar.gz
RUN sbt stage

FROM openjdk:11-jre-slim
WORKDIR /uexplorer
COPY --from=builder /mnt/modules/chain-indexer/target/universal/stage/ /uexplorer
ENTRYPOINT ["/uexplorer/bin/chain-indexer"]

# The instrumented ledger server (as Dockerfile.antithesis), run under
# forward-logs so its logs reach the driver. Build context: repository root.
FROM golang:1.27.3-trixie AS compiler
ARG BUILD_TAGS="s3,nats,enable_antithesis_sdk"
RUN apt-get update && apt-get install -y bash git gcc
# Keep this SHA equal to the antithesis-sdk-go replace in go.mod.
RUN git clone --quiet https://github.com/formancehq/antithesis-sdk-go /sdk \
 && git -C /sdk checkout --quiet 1c9afdaf82044c6c7b52d1feb751358034c98506 \
 && cd /sdk/tools/antithesis-go-instrumentor \
 && go build -o /usr/local/bin/antithesis-go-instrumentor .
WORKDIR /src
COPY go.mod go.sum ./
COPY main.go .
COPY internal internal
COPY cmd cmd
COPY pkg pkg
RUN mkdir /instrumented \
 && antithesis-go-instrumentor -instrumentor_version v0.8.0-default-no-op . /instrumented
WORKDIR /instrumented/customer
RUN go build -race -tags "${BUILD_TAGS}" -o /ledger-server .

FROM rust:1-trixie AS log-forward
RUN rustup target add x86_64-unknown-linux-musl
COPY tests/antithesis/hegel/log-forward/Cargo.toml /src/Cargo.toml
COPY tests/antithesis/hegel/log-forward/src /src/src
RUN cd /src && cargo build --release --target x86_64-unknown-linux-musl

FROM debian:trixie
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=UTC
WORKDIR /app
COPY --from=compiler /ledger-server /app/ledger-server
COPY --from=compiler /instrumented/symbols/ /symbols/
COPY --from=log-forward /src/target/x86_64-unknown-linux-musl/release/log-forward /usr/local/bin/
COPY tests/antithesis/hegel/forward-logs /usr/local/bin/
COPY tests/antithesis/hegel/ledger/start.sh /app/start.sh
RUN chmod 755 /usr/local/bin/forward-logs /app/start.sh
ENTRYPOINT ["forward-logs", "/app/start.sh"]

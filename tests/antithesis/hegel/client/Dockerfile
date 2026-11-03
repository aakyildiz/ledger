# The client program (tests/antithesis/workload/bin/client), instrumented like
# the workload drivers, run under forward-logs. Build context: repository root.
FROM golang:1.27.3-trixie AS compiler
RUN apt-get update && apt-get install -y bash git gcc
# Keep this SHA equal to the antithesis-sdk-go replace in go.mod.
RUN git clone --quiet https://github.com/formancehq/antithesis-sdk-go /sdk \
 && git -C /sdk checkout --quiet 1c9afdaf82044c6c7b52d1feb751358034c98506 \
 && cd /sdk/tools/antithesis-go-instrumentor \
 && go build -o /usr/local/bin/antithesis-go-instrumentor .
# The instrumentor writes to /instrumented/customer and the workload's
# replace (../../../) must resolve to the root module there, so the root
# module sits at /.
COPY go.mod go.sum /
COPY internal /internal
COPY pkg /pkg
COPY cmd/server /cmd/server
COPY tests/oracle /tests/oracle
COPY tests/antithesis/workload/go.mod tests/antithesis/workload/go.sum /src/
COPY tests/antithesis/workload/internal /src/internal
COPY tests/antithesis/workload/bin/client /src/bin/client
WORKDIR /src
RUN mkdir /instrumented && antithesis-go-instrumentor -instrumentor_version v0.8.0-default-no-op . /instrumented
WORKDIR /instrumented/customer
RUN go build -race -tags enable_antithesis_sdk -o /client ./bin/client

FROM rust:1-trixie AS log-forward
RUN rustup target add x86_64-unknown-linux-musl
COPY tests/antithesis/hegel/log-forward/Cargo.toml /src/Cargo.toml
COPY tests/antithesis/hegel/log-forward/src /src/src
RUN cd /src && cargo build --release --target x86_64-unknown-linux-musl

FROM debian:trixie
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=compiler /client /usr/local/bin/client
COPY --from=compiler /instrumented/symbols/ /symbols/
COPY --from=log-forward /src/target/x86_64-unknown-linux-musl/release/log-forward /usr/local/bin/
COPY tests/antithesis/hegel/forward-logs /usr/local/bin/
RUN chmod 755 /usr/local/bin/forward-logs
ENTRYPOINT ["forward-logs", "/usr/local/bin/client"]

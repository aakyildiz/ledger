# The hegel restore targets (restore-0, restore-1; task 18): the
# instrumented ledger server built WITHOUT -race (restore targets exist for
# parity, not race detection; DESIGN §2.4 memory budget), ledgerctl with the
# s3 tag (offline `store bootstrap`), and restore-agent, which supervises one
# server child on the driver's commands. Run under forward-logs, so the
# child's output (passed through by the agent) reaches the driver.
# Build context: repository root.
FROM golang:1.27.3-trixie AS server
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
RUN go build -tags "${BUILD_TAGS}" -o /ledger-server .
# ledgerctl is a tool here, not the system under test: not instrumented.
WORKDIR /src
RUN go build -tags s3 -o /ledgerctl ./cmd/ledgerctl

FROM golang:1.27.3-trixie AS agent
# The workload's replace (../../../) must resolve to the root module, so the
# root module sits at /. Only the agent is copied from the workload module
# (it imports nothing from workload/internal).
COPY go.mod go.sum /
COPY internal /internal
COPY pkg /pkg
COPY cmd/server /cmd/server
COPY tests/oracle /tests/oracle
COPY tests/antithesis/workload/go.mod tests/antithesis/workload/go.sum /src/
COPY tests/antithesis/workload/bin/restore-agent /src/bin/restore-agent
WORKDIR /src
RUN go build -tags s3 -o /restore-agent ./bin/restore-agent

FROM rust:1-trixie AS log-forward
RUN rustup target add x86_64-unknown-linux-musl
COPY tests/antithesis/hegel/log-forward/Cargo.toml /src/Cargo.toml
COPY tests/antithesis/hegel/log-forward/src /src/src
RUN cd /src && cargo build --release --target x86_64-unknown-linux-musl

FROM debian:trixie
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=UTC
WORKDIR /app
COPY --from=server /ledger-server /app/ledger-server
COPY --from=server /ledgerctl /app/ledgerctl
COPY --from=server /instrumented/symbols/ /symbols/
COPY --from=agent /restore-agent /app/restore-agent
COPY --from=log-forward /src/target/x86_64-unknown-linux-musl/release/log-forward /usr/local/bin/
COPY tests/antithesis/hegel/forward-logs /usr/local/bin/
RUN chmod 755 /usr/local/bin/forward-logs
EXPOSE 8090
ENTRYPOINT ["forward-logs", "/app/restore-agent"]

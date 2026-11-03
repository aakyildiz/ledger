# The hegel oracle service (tests/antithesis/workload/bin/oracle), instrumented
# and built WITHOUT -race (the candidate search is CPU-bound; race runs happen
# in `go test -race`). Its stdout is captured by Antithesis directly: the
# entrypoint is the oracle binary itself, no forward-logs. Build context:
# repository root.
#
# Only the root module's server packages, tests/oracle, and the workload
# module's oraclecheck and bin/oracle are copied — never workload/internal
# (DESIGN §5.1).
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
COPY tests/antithesis/workload/oraclecheck /src/oraclecheck
COPY tests/antithesis/workload/bin/oracle /src/bin/oracle
WORKDIR /src
# Test files import packages that are not copied (workload/internal/sdktest);
# the image needs none of them.
RUN find /src -name '*_test.go' -delete \
 && rm -rf /src/oraclecheck/testdata \
 && mkdir /instrumented \
 && antithesis-go-instrumentor -instrumentor_version v0.8.0-default-no-op . /instrumented
WORKDIR /instrumented/customer
RUN go build -tags enable_antithesis_sdk -o /oracle ./bin/oracle

FROM debian:trixie
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=compiler /oracle /usr/local/bin/oracle
COPY --from=compiler /instrumented/symbols/ /symbols/
ENV ORACLE_ADDR=:8090
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/oracle"]

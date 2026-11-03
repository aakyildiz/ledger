# The hegel driver. Build context: repository root.
FROM rust:1-trixie AS build
WORKDIR /src
COPY tests/antithesis/hegel/driver/Cargo.toml tests/antithesis/hegel/driver/Cargo.lock ./
COPY tests/antithesis/hegel/driver/src src
RUN cargo build --release --locked

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/driver /usr/local/bin/driver
COPY tests/antithesis/hegel/driver/first_start_workload /opt/antithesis/test/v1/main/
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]

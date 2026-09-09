# replog broker + controller image for the fault-injection harness.
# Built from the repository root: docker build -f deploy/replog/Dockerfile .
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin replog_broker --bin replog_controller \
    && mkdir -p /out && cp target/release/replog_broker target/release/replog_controller /out/

FROM debian:bookworm-slim
COPY --from=build /out/replog_broker /out/replog_controller /usr/local/bin/
VOLUME ["/data"]

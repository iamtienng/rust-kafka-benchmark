FROM rust:1.91.1-slim AS builder

# Install dependencies required by rdkafka-sys
RUN apt-get update && apt-get install -y \
  cmake \
  g++ \
  pkg-config \
  libssl-dev \
  openssl \
  zlib1g-dev \
  perl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf target/release/.fingerprint/* && rm -rf src

# Build actual app
COPY . .
RUN cargo build --release

# Runtime image
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
  libc6 \
  libssl3 \
  zlib1g \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/app/target/release/kafka-rust-benchmark .

CMD ["./kafka-rust-benchmark"]

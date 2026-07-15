FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY backend/Cargo.toml backend/Cargo.toml
COPY bench/Cargo.toml bench/Cargo.toml
RUN mkdir -p backend/src bench/src && printf 'fn main() {}\n' > backend/src/main.rs && printf 'fn main() {}\n' > bench/src/main.rs
RUN cargo build --release -p vussa
COPY backend backend
RUN touch backend/src/main.rs && cargo build --release -p vussa

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home vussa
WORKDIR /app
COPY --from=builder /src/target/release/vussa /usr/local/bin/vussa
RUN mkdir -p /var/lib/vussa/uploads && chown -R 10001:10001 /var/lib/vussa
USER 10001
ENV UPLOAD_DIR=/var/lib/vussa/uploads
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/vussa"]

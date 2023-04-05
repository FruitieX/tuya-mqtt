FROM rust:1.68 AS builder
WORKDIR /usr/src/add-bot
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/tuya-mqtt /usr/local/bin/tuya-mqtt
CMD ["tuya-mqtt"]

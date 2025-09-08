FROM gcr.io/distroless/static@sha256:87bce11be0af225e4ca761c40babb06d6d559f5767fbf7dc3c47f0f1a466b92c
COPY target/x86_64-unknown-linux-musl/release/tuya-mqtt /usr/local/bin/tuya-mqtt
CMD ["tuya-mqtt"]

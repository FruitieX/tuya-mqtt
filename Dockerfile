FROM gcr.io/distroless/static@sha256:f2ff10a709b0fd153997059b698ada702e4870745b6077eff03a5f4850ca91b6
COPY target/x86_64-unknown-linux-musl/release/tuya-mqtt /usr/local/bin/tuya-mqtt
CMD ["tuya-mqtt"]

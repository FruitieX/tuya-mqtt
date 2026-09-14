FROM gcr.io/distroless/static@sha256:58133991db06659feaabe0f4e97a35cebf15ef4ea08f8a4c6d2ee5f75e4aa6a0
COPY target/x86_64-unknown-linux-musl/release/tuya-mqtt /usr/local/bin/tuya-mqtt
CMD ["tuya-mqtt"]

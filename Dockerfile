# Shopping Sessions: native Rust pipeline.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon (Cascade Lake or newer): AVX2/BMI2 are safe.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && strip target/release/ss

FROM debian:bookworm-slim
COPY --from=build /src/target/release/ss /app/ss
USER 1000:1000
CMD ["/app/ss"]

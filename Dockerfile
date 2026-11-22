# Shopping Sessions: native Rust pipeline.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY .cargo ./.cargo


RUN cargo build --release --locked && strip target/release/ss

FROM debian:bookworm-slim
COPY --from=build /src/target/release/ss /app/ss
ENV MIMALLOC_PURGE_DELAY=-1
ENV SS_CHUNKMB=4
ENV SS_LINKTHREADS=8
USER 1000:1000
CMD ["/app/ss"]

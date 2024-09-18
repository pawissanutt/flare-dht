ARG APP_NAME="flare-dht"

FROM lukemathwalker/cargo-chef:latest as chef
WORKDIR /app

FROM chef AS planner
# COPY ./Cargo.toml ./Cargo.lock ./
COPY . .
RUN cargo chef prepare

FROM chef AS builder
RUN apt update && apt install -y protobuf-compiler
COPY --from=planner /app/recipe.json .
RUN cargo chef cook --release
COPY . .
RUN cargo build --release
RUN mv ./target/release/${APP_NAME} ./app

FROM debian:stable-slim AS runtime
WORKDIR /app
COPY --from=builder /app/app /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/app"]
CMD [ "server", "-p", "80"]
# FRONTEND BUILDER
FROM --platform=$BUILDPLATFORM node:22 AS vitebuilder
WORKDIR /app
COPY ./frontend .
RUN npm install && npm run build

# BACKEND BUILDER
FROM --platform=$BUILDPLATFORM rust:1 AS rustbuilder
ARG TARGETARCH
WORKDIR /app

# should write /app/.platform and /app/.compiler
COPY --chmod=555 deployment/platform.sh .
RUN ./platform.sh

# setup rust compilation for the target platform
RUN rustup component add rustfmt
RUN rustup target add $(cat /app/.platform)
RUN apt-get update && apt-get install -y $(cat /app/.compiler) protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY deployment/cargo-config.toml ./.cargo/config

# Compile the backend
COPY ./backend .
RUN cargo build --release --target $(cat /app/.platform)
RUN cp /app/target/$(cat /app/.platform)/release/lores-websites /app/backend-bin

# RUNNER
FROM ubuntu AS runner
COPY --from=rustbuilder /app/backend-bin /app/backend/backend
COPY --from=vitebuilder /app/dist /app/frontend
ENV FRONTEND_PATH=/app/frontend
EXPOSE 3000
WORKDIR /app/backend
CMD ["./backend"]

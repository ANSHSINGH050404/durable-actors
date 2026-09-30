FROM rust:1.89.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY .cargo ./.cargo
COPY migrations ./migrations
COPY proto ./proto
COPY src ./src
COPY docs/reference/openapi.yaml ./docs/reference/openapi.yaml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release \
    && mkdir -p /out \
    && cp target/release/durable-actors /out/durable-actors

FROM node:22.19.0-bookworm AS sdk-builder
WORKDIR /build
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY sdk/package.json ./sdk/package.json
COPY packages/observer-ui/package.json ./packages/observer-ui/package.json
COPY examples/chat/package.json ./examples/chat/package.json
COPY examples/ai-chat/package.json ./examples/ai-chat/package.json
COPY examples/documents/package.json ./examples/documents/package.json
RUN corepack enable && pnpm install --frozen-lockfile
COPY packages/observer-ui ./packages/observer-ui
COPY sdk/src ./sdk/src
COPY sdk/scripts/build-client-runtime.mjs ./sdk/scripts/build-client-runtime.mjs
COPY sdk/LICENSE.md ./sdk/LICENSE.md
COPY sdk/tsconfig*.json ./sdk/
RUN pnpm --dir packages/observer-ui build \
    && pnpm --dir sdk build:client \
    && pnpm --dir sdk exec tsc -p tsconfig.build.json

FROM python:3.13-slim-bookworm AS python-sdk
WORKDIR /build
COPY sdk-python/pyproject.toml sdk-python/README.md sdk-python/LICENSE.md ./
COPY sdk-python/src ./src
RUN pip install --no-cache-dir .

FROM oven/bun:1.4.2 AS bun

FROM python:3.13-slim-bookworm

RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-sdk /usr/local /usr/local
COPY --from=builder /out/durable-actors /usr/local/bin/durable-actors
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
COPY --from=sdk-builder /build/node_modules /opt/durable-actors/node_modules
COPY --from=sdk-builder /build/sdk/node_modules /opt/durable-actors/sdk/node_modules
COPY --from=sdk-builder /build/packages/observer-ui /opt/durable-actors/packages/observer-ui
COPY --from=sdk-builder /build/sdk/dist /opt/durable-actors/sdk/dist
COPY sdk/package.json /opt/durable-actors/sdk/package.json
RUN mkdir -p /customer /node_modules \
    && ln -s /opt/durable-actors/sdk /node_modules/durable-actors
RUN useradd --uid 10000 --create-home --home-dir /home/runtime runtime
USER 10000:10000

ENV RUST_LOG=warn,durable_actors=info
ENV DURABLE_ACTORS_SDK_HOST=/opt/durable-actors/sdk/dist/host.js
ENTRYPOINT ["/usr/local/bin/durable-actors"]

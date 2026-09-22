# Minimal preloop-runner image: the runner binary plus the tools a job needs
# to do anything useful (git for checkouts, curl, certs). Node externals are
# fetched by `preloop-runner configure` unless --no-externals is passed.
FROM ubuntu:24.04

ARG TARGETARCH
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
       git curl ca-certificates unzip \
    && rm -rf /var/lib/apt/lists/*

# Docker CLI pinned to the same versions the official runner image ships, so
# client/daemon negotiation behaves identically. Values come from
# versions.toml (runner_image_docker_version / runner_image_buildx_version),
# passed as build-args by release-runner.yml — no defaults here so a local
# build without them fails loudly instead of drifting.
ARG DOCKER_VERSION
ARG BUILDX_VERSION
RUN test -n "$DOCKER_VERSION" && test -n "$BUILDX_VERSION" \
    || { echo "DOCKER_VERSION/BUILDX_VERSION required — see versions.toml" >&2; exit 1; }
RUN case "$TARGETARCH" in \
      amd64) docker_arch=x86_64 ;; \
      arm64) docker_arch=aarch64 ;; \
      *) echo "unsupported arch: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${docker_arch}/docker-${DOCKER_VERSION}.tgz" \
       | tar xz -C /tmp docker/docker \
    && install -m 0755 /tmp/docker/docker /usr/local/bin/docker \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/buildx/releases/download/v${BUILDX_VERSION}/buildx-v${BUILDX_VERSION}.linux-${TARGETARCH}" \
       -o /usr/local/lib/docker/cli-plugins/docker-buildx \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx \
    && rm -rf /tmp/docker


COPY dist/ /tmp/dist/
RUN case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported arch: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && install -m 0755 "/tmp/dist/$triple/preloop-runner" /usr/local/bin/preloop-runner \
    && rm -rf /tmp/dist

ENTRYPOINT ["preloop-runner"]
CMD ["run"]

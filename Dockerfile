# Digest pins (2026-11-24, review low): golang:1.27-alpine and
# distroless/static-debian12:nonroot index digests; bump by re-resolving the
# tags, not by silently re-pulling.
FROM --platform=$BUILDPLATFORM golang@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download
COPY . .
# One build step for every cmd/* binary (they are named after their
# directory), with a cache mount and -trimpath: eight separate RUNs rebuilt
# the whole module serially with no cache (review low).
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2 AS runtime
USER nonroot
COPY --from=build /out/api /api
COPY --from=build /out/mockworker /mockworker
COPY --from=build /out/scheduler /scheduler
COPY --from=build /out/adapter /adapter
COPY --from=build /out/vllmworker /vllmworker
COPY --from=build /out/fakegpu /fakegpu
COPY --from=build /out/traffic /traffic
COPY --from=build /out/datasets /datasets
EXPOSE 8080
ENTRYPOINT ["/api"]

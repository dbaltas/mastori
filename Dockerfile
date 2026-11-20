FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /api ./cmd/api
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /mockworker ./cmd/mockworker
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /scheduler ./cmd/scheduler
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /adapter ./cmd/adapter
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /vllmworker ./cmd/vllmworker
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /fakegpu ./cmd/fakegpu
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /traffic ./cmd/traffic
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /datasets ./cmd/datasets

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /api /api
COPY --from=build /mockworker /mockworker
COPY --from=build /scheduler /scheduler
COPY --from=build /adapter /adapter
COPY --from=build /vllmworker /vllmworker
COPY --from=build /fakegpu /fakegpu
COPY --from=build /traffic /traffic
COPY --from=build /datasets /datasets
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/api"]

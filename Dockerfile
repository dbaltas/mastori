FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /api ./cmd/api
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /mockworker ./cmd/mockworker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /api /api
COPY --from=build /mockworker /mockworker
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/api"]

# Build stage: static binary, no cgo.
FROM golang:1.24-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/orders-consumer ./cmd/orders-consumer

# Runtime stage: distroless, non-root, no shell.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/orders-consumer /usr/local/bin/orders-consumer
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/orders-consumer"]

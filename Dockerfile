# RapidoDB Dockerfile
# Build and benchmark RapidoDB

FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make

# Copy source code
COPY . .

# Build
RUN go build -o /rapidodb-server ./cmd/server
RUN go build -o /rapidodb-bench ./cmd/bench

# Runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /rapidodb-server /usr/local/bin/
COPY --from=builder /rapidodb-bench /usr/local/bin/

# Create data directory
RUN mkdir -p /data

WORKDIR /data

# Default: run benchmark
ENTRYPOINT ["rapidodb-bench"]
CMD ["--mode", "all", "--num", "100000", "--data-dir", "/data/benchmark"]

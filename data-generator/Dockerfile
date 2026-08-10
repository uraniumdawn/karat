FROM golang:1.24 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Static binary for whatever architecture the build runs on.
RUN CGO_ENABLED=0 GOOS=linux go build -o data-generator main.go

# Minimal runtime image
FROM gcr.io/distroless/static:nonroot

WORKDIR /app
COPY --from=builder /app/data-generator .
ENTRYPOINT ["/app/data-generator"]

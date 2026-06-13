FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o weather ./cmd/weather

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/weather /weather
# config.yaml is mounted at runtime via docker-compose volume — never bake it into the image.
ENTRYPOINT ["/weather", "--config", "/config.yaml"]

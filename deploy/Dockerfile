FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o weather-server ./cmd/weather-server

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/weather-server /weather-server
# config.yaml is mounted at runtime via docker-compose volume — never bake it into the image.
ENTRYPOINT ["/weather-server", "--config", "/config.yaml"]

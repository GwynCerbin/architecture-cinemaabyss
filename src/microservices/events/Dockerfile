FROM golang:1.27.3-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /events-worker ./cmd/events-worker

FROM alpine:3.22
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /events-worker ./events-worker
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["./events-worker"]

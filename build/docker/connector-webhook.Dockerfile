FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/connector-webhook ./connectors/webhook/cmd/connector-webhook

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/connector-webhook /app/connector-webhook
EXPOSE 8093
ENTRYPOINT ["/app/connector-webhook"]

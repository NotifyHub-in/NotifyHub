FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/callback-gateway ./apps/callback-gateway/cmd/callback-gateway

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/callback-gateway /app/callback-gateway
EXPOSE 8082
ENTRYPOINT ["/app/callback-gateway"]

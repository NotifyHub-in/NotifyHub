FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/connector-push ./connectors/push/cmd/connector-push

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/connector-push /app/connector-push
EXPOSE 8094
ENTRYPOINT ["/app/connector-push"]

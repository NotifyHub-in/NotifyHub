FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/connector-whatsapp ./connectors/whatsapp/cmd/connector-whatsapp

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/connector-whatsapp /app/connector-whatsapp
EXPOSE 8095
ENTRYPOINT ["/app/connector-whatsapp"]

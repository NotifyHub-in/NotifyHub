FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/connector-email ./connectors/email/cmd/connector-email

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/connector-email /app/connector-email
EXPOSE 8091
ENTRYPOINT ["/app/connector-email"]

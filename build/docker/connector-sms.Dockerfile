FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/connector-sms ./connectors/sms/cmd/connector-sms

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/connector-sms /app/connector-sms
EXPOSE 8092
ENTRYPOINT ["/app/connector-sms"]

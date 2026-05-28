FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/migrate ./apps/migrate/cmd/migrate

FROM alpine:3.22
RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/migrate /app/migrate
COPY --from=builder /src/migrations /app/migrations
ENTRYPOINT ["/app/migrate"]

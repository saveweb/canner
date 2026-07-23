# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/canner .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/canner /app/canner
EXPOSE 8080
ENTRYPOINT ["/app/canner"]
CMD ["serve", "/app/config.json"]

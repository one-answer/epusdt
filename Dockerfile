FROM golang:alpine AS builder

RUN apk add --no-cache --update git build-base
ENV CGO_ENABLED=0

WORKDIR /app

RUN git clone https://github.com/one-answer/epusdt.git .

WORKDIR /app/src
RUN go mod download
RUN go build -o /app/epusdt .

FROM alpine:latest AS runner
ENV TZ=Asia/Shanghai
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/src/static /app/static
COPY --from=builder /app/src/static /static
COPY --from=builder /app/epusdt .

VOLUME /app/conf
ENTRYPOINT ["./epusdt", "http", "start"]

FROM --platform=$BUILDPLATFORM golang:1.24.8-alpine3.22 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z

RUN apk add --no-cache git
WORKDIR /src
RUN git init . \
    && git remote add origin https://github.com/minio/minio.git \
    && git fetch --depth 1 origin "${MINIO_RELEASE}" \
    && git checkout FETCH_HEAD

RUN mkdir -p /out \
    && CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -tags kqueue -trimpath \
    -ldflags "$(go run buildscripts/gen-ldflags.go)" \
    -o /out/minio .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S minio \
    && adduser -S -G minio minio \
    && mkdir -p /data \
    && chown minio:minio /data

COPY --from=builder /out/minio /usr/local/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]
USER minio
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]

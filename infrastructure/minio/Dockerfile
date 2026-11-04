FROM golang:1.24.8-alpine3.22 AS builder

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

ENV GOBIN=/usr/local/bin
RUN go install github.com/minio/minio@${MINIO_VERSION} \
    && go install github.com/minio/mc@${MC_VERSION}

FROM alpine:3.22.1

COPY --from=builder /usr/local/bin/minio /usr/local/bin/minio
COPY --from=builder /usr/local/bin/mc /usr/local/bin/mc
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

EXPOSE 9000 9001
ENTRYPOINT ["minio"]

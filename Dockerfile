# syntax=docker/dockerfile:1

FROM --platform=${BUILDPLATFORM} golang:1.22-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -ldflags="-w -s -buildid=" -trimpath -o /out/app .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

COPY --link --from=builder --chown=65532:65532 /out/app /opt/app

USER 65532:65532

EXPOSE 8090

ENTRYPOINT [ "/opt/app" ]

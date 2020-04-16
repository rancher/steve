# syntax = docker/dockerfile:experimental
FROM golang:1.13.4 as build
COPY go.mod go.sum main.go /src/
COPY pkg /src/pkg/
#RUN --mount=type=cache,target=/root/.cache/go-build \
RUN \
    cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /steve

FROM alpine
RUN apk -U --no-cache add ca-certificates
COPY --from=build /steve /usr/bin/steve
# Hack to make golang do files,dns search order
ENV LOCALDOMAIN=""
ENTRYPOINT ["/usr/bin/steve"]

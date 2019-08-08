FROM golang:1.12.7 as build
COPY go.mod  go.sum main.go /src/
COPY vendor /src/vendor/
COPY pkg /src/pkg/
RUN cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /naok -mod=vendor

FROM alpine
RUN apk -U --no-cache add ca-certificates
COPY --from=build /naok /usr/bin/naok
# Hack to make golang do files,dns search order
ENV LOCALDOMAIN=""
ENTRYPOINT ["/usr/bin/naok"]

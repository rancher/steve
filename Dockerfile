FROM golang:1.12 as build
COPY go.mod  go.sum main.go /src/
COPY vendor /src/vendor/
COPY pkg /src/pkg/
RUN cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /naok -mod=vendor

FROM alpine
COPY --from=build /naok /usr/bin/naok
ENTRYPOINT ["/usr/bin/naok"]

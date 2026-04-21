# syntax = docker/dockerfile:experimental
FROM registry.suse.com/bci/golang:1.25@sha256:3190609b31bbdbd4396ecba6c17ef71ccd282ec6973dba7c106b391b3edb1807 as build
COPY go.mod go.sum main.go /src/
COPY pkg /src/pkg/
#RUN --mount=type=cache,target=/root/.cache/go-build \
RUN \
    cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /steve

FROM registry.suse.com/bci/bci-micro:15.7@sha256:590f1bead5a54cfd3cee7b7d4459f874f3e1b2789a7d2149a2d80b35a43ac9cf

ARG user=steve

RUN echo "$user:x:1000:1000::/home/$user:/bin/bash" >> /etc/passwd && \
    echo "$user:x:1000:" >> /etc/group && \
    mkdir /home/$user && \
    chown -R $user:$user /home/$user

COPY --from=build /steve /usr/bin/steve
# Hack to make golang do files,dns search order
ENV LOCALDOMAIN=""
USER $user
ENTRYPOINT ["/usr/bin/steve"]

# syntax = docker/dockerfile:experimental
FROM registry.suse.com/bci/golang:1.25@sha256:71eb104cb6375c29d2c552f1470fafb040d2ab2b68324c00076b0255ecdd0989 as build
COPY go.mod go.sum main.go /src/
COPY pkg /src/pkg/
#RUN --mount=type=cache,target=/root/.cache/go-build \
RUN \
    cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /steve

FROM registry.suse.com/bci/bci-micro:15.7@sha256:5ca1a44ca5be8afd3e4abc721abf1efe6b0fe69b83cf01a0c204c16160913edc

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

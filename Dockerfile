# syntax = docker/dockerfile:experimental
FROM registry.suse.com/bci/golang:1.25@sha256:c1a6327cfa677209e87db4737d350a441786e0afe4e78fcf9c2bdc7c1fc67e16 as build
COPY go.mod go.sum main.go /src/
COPY pkg /src/pkg/
#RUN --mount=type=cache,target=/root/.cache/go-build \
RUN \
    cd /src && \
    CGO_ENABLED=0 go build -ldflags "-extldflags -static -s" -o /steve

FROM registry.suse.com/bci/bci-micro:15.7@sha256:07fce181ac208790fe3986e23ff9f1ded5ec6a25259e12631d835d31c57b7245

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

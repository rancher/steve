build:
	docker build -t naok .

run: build
	docker run $(DOCKER_ARGS) --rm -p 8989:8989 -it -v ${HOME}/.kube:/root/.kube naok

run-host: build
	docker run $(DOCKER_ARGS) --net=host --rm -it -v ${HOME}/.kube:/root/.kube naok

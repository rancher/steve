build:
	docker build -t naok .

run: build
	docker run $(DOCKER_ARGS) --rm -it -v ${HOME}/.kube:/root/.kube naok

run-host: build
	docker run $(DOCKER_ARGS) --net=host --rm -it -v ${HOME}/.kube:/root/.kube naok

build:
	docker build -t naok .

run: build
	docker run $(DOCKER_ARGS) --rm -p 8989:8080 -it -v ${HOME}/.kube:/root/.kube naok

run-host: build
	docker run $(DOCKER_ARGS) --net=host --uts=host --rm -it -v ${HOME}/.kube:/root/.kube naok --kubeconfig /root/.kube/config --listen-address :8989

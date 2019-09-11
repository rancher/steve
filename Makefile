build:
	docker build -t steve .

run: build
	docker run $(DOCKER_ARGS) --rm -p 8989:8080 -it -v ${HOME}/.kube:/root/.kube steve

run-host: build
	docker run $(DOCKER_ARGS) --net=host --uts=host --rm -it -v ${HOME}/.kube:/root/.kube steve --kubeconfig /root/.kube/config --listen-address :8989

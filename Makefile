INTEGRATION_TEST_ARGS ?=
INTEGRATION_TEST_DEBUG ?=

TEST_ARGS := $(INTEGRATION_TEST_ARGS)

ifeq ($(INTEGRATION_TEST_DEBUG),true)
# we likely want longer timeout
TEST_ARGS += -timeout=60m
# verbose to see KUBECONFIG path and steve URL
TEST_ARGS += -v
endif

build:
	docker build -t steve .

build-bin:
	bash scripts/build-bin.sh

run: build
	docker run $(DOCKER_ARGS) --rm -p 8989:9080 -it -v ${HOME}/.kube:/root/.kube steve --https-listen-port 0

run-host: build
	docker run $(DOCKER_ARGS) --net=host --uts=host --rm -it -v ${HOME}/.kube:/root/.kube steve --kubeconfig /root/.kube/config --http-listen-port 8989 --https-listen-port 0

test:
	bash scripts/test.sh

integration-tests:
	@TEST_ARGS="$(TEST_ARGS)" ./scripts/integration-tests.sh

validate:
	bash scripts/validate.sh

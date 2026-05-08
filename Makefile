INTEGRATION_TEST_ARGS ?=
INTEGRATION_TEST_DEBUG ?=

TEST_ARGS := $(INTEGRATION_TEST_ARGS)

ifeq ($(INTEGRATION_TEST_DEBUG),true)
# we likely want longer timeout
TEST_ARGS += -timeout=60m
# verbose to see KUBECONFIG path and steve URL
TEST_ARGS += -v
endif

build-bin:
	bash scripts/build-bin.sh

test:
	bash scripts/test.sh

integration-tests:
	@TEST_ARGS="$(TEST_ARGS)" ./scripts/integration-tests.sh

validate:
	bash scripts/validate.sh

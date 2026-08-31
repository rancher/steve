# Integration Tests

This document outlines how to run, debug and add new integration tests for steve.

## Dependencies

To run the integration tests, you will need a running Kubernetes cluster. The tests will automatically target the cluster configured in your `KUBECONFIG` environment variable.

If you do not have a cluster configured, the tests will attempt to create a local one for you using [k3d](https://k3d.io/v5.6.0/#installation). This requires the following tools to be installed:

- [docker](https://docs.docker.com/get-docker/)
- [k3d](https://k3d.io/v5.6.0/#installation)

You will also need [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) to be installed and available in your `PATH`.

## Running the tests

The integration tests can be run by executing the following command:

```sh
make integration-tests
```

This will:
1. Connect to the Kubernetes cluster defined by your `KUBECONFIG` environment variable. If no cluster is configured, it will try to create a `k3d` cluster named `steve-integration-test`.
2. Run the Go tests located in the `tests/` directory.

## Debugging

It is possible to pause the execution of a test to manually inspect the state of the cluster, the steve API and the SQLite database. This can be achieved by setting `INTEGRATION_TEST_DEBUG=true`:

```sh
INTEGRATION_TEST_DEBUG=true make integration-tests
```

When a test is finished, it will print the following information and block until the test is cancelled (e.g. with `Ctrl+C`):

```
###########################
#
# Integration tests stopped as requested
#
# You can now access the Kubernetes cluster and steve
#
# Kubernetes: KUBECONFIG=<path to kubeconfig>
# Steve URL: <steve API endpoint>
# SQL cache database: <path to sqlite database>
#
###########################
```

You can then use this information to interact with the Kubernetes cluster and the steve API.

## Adding new tests

The integration tests are written using the `testify/suite` package. The main suite is `IntegrationSuite` in `integration_test.go`.

Tests are data-driven and rely on `.test.yaml` files located in the `testdata` directory. These YAML files contain both the Kubernetes resources to apply and the configuration for the test assertions.

To add a new test scenario, you can follow the example of `column_test.go` and `testdata/columns/`:

1.  Create a new `my_feature_test.go` file in the `tests/` directory.
2.  Create a new directory `testdata/my_feature/`.
3.  Add a new test function to the `IntegrationSuite`, for example `TestMyFeature`, in your new Go file.
4.  In this function, you will typically:
    -   Set up a `httptest.NewServer` with the steve API handler.
    -   Find your test scenarios by looking for `.test.yaml` files in `testdata/my_feature/`.
    -   For each scenario, run a subtest.
5.  Each subtest should:
    -   Parse the `.test.yaml` file. The file can contain a header with test configuration and Kubernetes objects separated by `---`.
    -   Apply the Kubernetes objects to the cluster.
    -   Make requests to the steve API.
    -   Assert that the responses are correct based on the test configuration.
    -   Ensure that `defer i.maybeStopAndDebug(baseURL)` is called to allow for debugging.

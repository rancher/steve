// This program generates the code for the test types and clients.
package main

import (
	"os"

	controllergen "github.com/rancher/wrangler/v3/pkg/controller-gen"
	"github.com/rancher/wrangler/v3/pkg/controller-gen/args"
)

func main() {
	os.Unsetenv("GOPATH")

	controllergen.Run(args.Options{
		OutputPackage: "github.com/rancher/steve/pkg/generated",
		Boilerplate:   "scripts/boilerplate.go.txt",
		Groups: map[string]args.Group{
			"testapis": {
				PackageName: "testapis",
				Types: []interface{}{
					// All structs with an embedded ObjectMeta field will be picked up
					"./pkg/ext/testapis/v1",
				},
				GenerateOpenAPI: true,
				GenerateTypes:   true,
				OpenAPIDependencies: []string{
					"k8s.io/apimachinery/pkg/apis/meta/v1",
					"k8s.io/apimachinery/pkg/runtime",
					"k8s.io/apimachinery/pkg/version",
				},
			},
		},
	})
}

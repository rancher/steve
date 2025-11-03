//go:generate msgp -unexported -o types.msgp.go

// go install github.com/tinylib/msgp@latest to get msgp

package db

type unstructuredObject map[string]any

.PHONY: all
all: protos

.PHONY: protos
protos:
	protoc --proto_path=helmv2api --go_out=helmv2api --go_opt=paths=source_relative helmv2api/*.proto

.PHONY: clean
clean:
	@rm -rf helmv2api/*.pb.go 2>/dev/null

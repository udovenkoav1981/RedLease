PROTO_ROOT := proto
PROTO_FILE := redlease/v1/redlease.proto

.PHONY: generate generate-proto test

generate: generate-proto

generate-proto:
	@redlease_protoc_gen_go="$$(go tool -n protoc-gen-go)"; \
	redlease_protoc_gen_go_grpc="$$(go tool -n protoc-gen-go-grpc)"; \
	protoc \
		--proto_path="$(PROTO_ROOT)" \
		--plugin=protoc-gen-go="$$redlease_protoc_gen_go" \
		--plugin=protoc-gen-go-grpc="$$redlease_protoc_gen_go_grpc" \
		--go_out="$(PROTO_ROOT)" \
		--go_opt=paths=source_relative \
		--go-grpc_out="$(PROTO_ROOT)" \
		--go-grpc_opt=paths=source_relative \
		"$(PROTO_ROOT)/$(PROTO_FILE)"

test:
	go test ./...

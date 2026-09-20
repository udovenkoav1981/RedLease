FLATBUFFERS_SCHEMA := proto/redlease/v1/redlease.fbs
FLATBUFFERS_OUTPUT := proto/redlease/v1

.PHONY: all build lint generate generate-flatbuffers test

all: build

build:
	go build ./...

lint:
	golangci-lint run ./...

generate: generate-flatbuffers

generate-flatbuffers:
	flatc --go --gen-onefile --go-namespace redleasev1 \
		-o "$(FLATBUFFERS_OUTPUT)" \
		"$(FLATBUFFERS_SCHEMA)"

test:
	go test ./...

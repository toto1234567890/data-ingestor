#!/bin/bash

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed."
    echo "Please install it using: brew install protobuf"
    exit 1
fi

# Check if Go plugins are installed
if ! command -v protoc-gen-go &> /dev/null || ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Error: Go plugins for protoc are not installed."
    echo "Please install them using:"
    echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
    exit 1
fi

echo "Generating gRPC code..."
protoc --proto_path=src/grpc_control --go_out=. --go-grpc_out=. src/grpc_control/grpc_control.proto


if [ $? -eq 0 ]; then
    echo "✅ Successfully generated gRPC code."
else
    echo "❌ Failed to generate gRPC code."
    exit 1
fi

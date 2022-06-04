#!/bin/bash

ROOT="$(dirname "$0")"
cd "$ROOT/.."

# Build go server.
rm -rf bin && mkdir bin
go build -o bin/server cmd/main.go

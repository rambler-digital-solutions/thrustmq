#!/bin/sh

./format.sh
go test ./test/consumer_test.go
go test -bench=. -benchmem ./test/benchmark_test.go > .bench
git add -A
git diff --cached

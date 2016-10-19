#!/bin/sh

./format.sh
go test ./tests/...
go test -bench=. -benchmem ./benchmarks/... > .bench
git add -A
git diff --cached

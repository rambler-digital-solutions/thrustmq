find . -name '*.py' | xargs autopep8 --in-place
find . -name '*.go' | xargs gofmt -w

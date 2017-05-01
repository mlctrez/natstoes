#!/usr/bin/env bash

rsync -a --delete ../natstoes search:go/src/github.com/mlctrez/

ssh search /usr/local/go/bin/go install github.com/mlctrez/natstoes

ssh -t search sudo cp go/bin/natstoes /usr/local/bin


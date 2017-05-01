#!/usr/bin/env bash

rsync -a --delete ../natstoes search:go/src/github.com/mlctrez/

ssh search /usr/local/go/bin/go install github.com/mlctrez/natstoes

ssh search sudo supervisorctl stop natstoes
ssh search sudo cp go/bin/natstoes /usr/local/bin
ssh search sudo supervisorctl start natstoes


#!/bin/bash

set -eux

GO=go1.18.6

if !which $GO
then
    go install golang.org/dl/go1.18.6@latest
    $GO download
fi

cat go.mod | grep -v 'gitlab.bixin.com' | tee go.mod.1 && mv go.mod.1 go.mod
$GO clean -modcache
$GO mod tidy

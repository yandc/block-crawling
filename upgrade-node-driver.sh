#!/bin/bash

set -eux

GO=go1.18.6

if !which $GO
then
    go install golang.org/dl/go1.18.6@latest
    $GO download
fi

$GO get gitlab.bixin.com/mili/node-driver

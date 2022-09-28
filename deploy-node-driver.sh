#!/bin/bash
set -eu

PWD=$(pwd)
DIR=$(dirname $PWD)

NODE_DRIVER_VER=$(cat go.mod | grep node-driver | grep -v ^replace | awk -F- '{print $4}')
NODE_DRIVER_REPO=$(cat go.mod | grep node-driver | grep -v ^replace | awk '{print $1}')
NODE_DRIVER_PATH=$DIR/${PWD##*/}-node-driver

if [[ ! -e "$NODE_DRIVER_PATH" ]]
then
    git clone https://$NODE_DRIVER_REPO $NODE_DRIVER_PATH
else
    pushd $NODE_DRIVER_PATH
    git fetch
    popd
fi

pushd $NODE_DRIVER_PATH
git reset --hard $NODE_DRIVER_VER
popd

if grep 'replace' go.mod | grep "$NODE_DRIVER_REPO"
then
    echo "node-driver already had been replaced."
    exit 0
fi

echo "replace $NODE_DRIVER_REPO => $NODE_DRIVER_PATH" | tee -a go.mod

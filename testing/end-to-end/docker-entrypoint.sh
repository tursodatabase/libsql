#!/bin/sh
# start podman API service

export SQLD_TEST_PODMAN_ADDR="unix:///var/run/podman.sock"
podman system service -t 0 $SQLD_TEST_PODMAN_ADDR&

podman load -i sqld.tar

## macos docker does not let us build in the mounted volume due to limitations on the number of FD
mkdir build
cp -r end-to-end/src build/
cp -r end-to-end/integration build/
cp -r end-to-end/Cargo.toml build/
cp -r end-to-end/Cargo.lock build/

cd build


SQLD_TEST_RUN=true cargo insta test

rsync -r integration ../end-to-end

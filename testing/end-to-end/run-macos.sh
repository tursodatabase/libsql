#!/bin/sh

podman build ../.. -t sqld
podman save -o sqld.tar sqld
podman build . -t end-to-end --isolation=chroot
podman run --privileged -v $PWD:/end-to-end -it --rm end-to-end
rm sqld.tar

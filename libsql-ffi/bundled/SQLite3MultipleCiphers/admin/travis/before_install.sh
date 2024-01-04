#!/bin/sh
#
# This script is used in .travis.yml to install the dependencies before building.
# Notice that WXGTK_PACKAGE is supposed to be defined before running it.

set -e

case "$TRAVIS_OS_NAME" in
    linux)
        sudo apt-get -qq update
        sudo apt-get install -y $WXGTK_PACKAGE
    ;;

    osx)
        brew update
        brew install wxmac
    ;;

    *)
        echo "Add commands to install wxWidgets on this platform!"
        exit 1
    ;;
esac

#! /bin/bash

# a hacky script to make all the various ios targets.
# once we have something consistently working we'll streamline all of this.

# Make all the non-simulator libs
# Package into a universal ios lib
mkdir -p ./dist-ios

# TODO: fix things up to not require a clean before each target.
make clean
export IOS_TARGET=aarch64-apple-ios; make static
cp ./dist/crsqlite-aarch64-apple-ios.a ./dist-ios

make clean
export IOS_TARGET=armv7-apple-ios; make static
cp ./dist/crsqlite-armv7-apple-ios.a ./dist-ios

make clean
export IOS_TARGET=armv7s-apple-ios; make static
cp ./dist/crsqlite-armv7s-apple-ios.a ./dist-ios

cd ./dist-ios
lipo crsqlite-aarch64-apple-ios.a crsqlite-armv7-apple-ios.a crsqlite-armv7s-apple-ios.a -create -output crsqlite-universal-ios.a

cd ..
# ===

# Make the simlator libs
# Package into a universal ios sim lib
mkdir -p ./dist-ios-sim

make clean
export IOS_TARGET=aarch64-apple-ios-sim; make static
cp ./dist/crsqlite-aarch64-apple-ios-sim.a ./dist-ios-sim

make clean
export IOS_TARGET=x86_64-apple-ios; make static
cp ./dist/crsqlite-x86_64-apple-ios.a ./dist-ios-sim

cd ./dist-ios-sim
lipo crsqlite-aarch64-apple-ios-sim.a crsqlite-x86_64-apple-ios.a -create -output crsqlite-universal-ios-sim.a

cd ..
# ===

# Make the macos static lib
mkdir -p ./dist-macos
make clean
unset IOS_TARGET
export CI_MAYBE_TARGET="aarch64-apple-darwin"; make static

cp ./dist/crsqlite-aarch64-apple-darwin.a ./dist-macos


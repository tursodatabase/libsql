#!/usr/bin/env bash

set -x

CMAKE_CROSS=""

CROSS_CC_VAR_NAME="CC_$CARGO_BUILD_TARGET"
CROSS_CC_VAR_NAME="${CROSS_CC_VAR_NAME//-/_}"
CROSS_CC="${!CROSS_CC_VAR_NAME}"

CROSS_CXX_VAR_NAME="CXX_$CARGO_BUILD_TARGET"
CROSS_CXX_VAR_NAME="${CROSS_CXX_VAR_NAME//-/_}"
CROSS_CXX="${!CROSS_CXX_VAR_NAME}"

if [[ $CROSS_CC == *aarch64*linux* ]]; then
  CMAKE_CROSS="-DCMAKE_TOOLCHAIN_FILE=../../toolchain.cmake"
  echo "set(CMAKE_SYSTEM_NAME \"Linux\")" > toolchain.cmake
  echo "set(CMAKE_SYSTEM_PROCESSOR \"arm64\")" >> toolchain.cmake
fi

if [ -n "$CROSS_CC" ]; then
  echo "set(CMAKE_C_COMPILER $CROSS_CC)" >> toolchain.cmake
fi

if [ -n "$CROSS_CXX" ]; then
  echo "set(CMAKE_CXX_COMPILER $CROSS_CXX)" >> toolchain.cmake
fi

git submodule update --init SQLite3MultipleCiphers
rm -rf SQLite3MultipleCiphers/build
mkdir -p SQLite3MultipleCiphers/build
cd SQLite3MultipleCiphers/build

CMAKE_OPTS="$CMAKE_CROSS"
CMAKE_OPTS+=" -DCMAKE_BUILD_TYPE=Release"
CMAKE_OPTS+=" -DSQLITE3MC_STATIC=ON"
CMAKE_OPTS+=" -DCODEC_TYPE=AES256"
CMAKE_OPTS+=" -DSQLITE3MC_BUILD_SHELL=OFF"
CMAKE_OPTS+=" -DSQLITE_SHELL_IS_UTF8=OFF"
CMAKE_OPTS+=" -DSQLITE_USER_AUTHENTICATION=OFF"
CMAKE_OPTS+=" -DSQLITE_SECURE_DELETE=OFF"
CMAKE_OPTS+=" -DSQLITE_ENABLE_COLUMN_METADATA=ON"
CMAKE_OPTS+=" -DSQLITE_USE_URI=ON"
CMAKE_OPTS+=" -DCMAKE_POSITION_INDEPENDENT_CODE=ON"

cmake $CMAKE_CROSS .. $CMAKE_OPTS $@

make -j12

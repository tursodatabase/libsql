#! /bin/bash

# a hacky script to make all the various ios targets.
# once we have something consistently working we'll streamline all of this.

BUILD_DIR=./build
DIST_PACKAGE_DIR=./dist

function createXcframework() {
  plist=$(cat << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>crsqlite</string>
  <key>CFBundleIdentifier</key>
  <string>io.vlcn.crsqlite</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundlePackageType</key>
  <string>FMWK</string>
  <key>CFBundleSignature</key>
  <string>????</string>
</dict>
</plist>
EOF
)
  printf "\n\n\t\t===================== create ios device framework =====================\n\n"
  mkdir -p "${BUILD_DIR}/ios-arm64/crsqlite.framework"
  echo "${plist}" > "${BUILD_DIR}/ios-arm64/crsqlite.framework/Info.plist"
  cp -f "./dist-ios/crsqlite-aarch64-apple-ios.dylib" "${BUILD_DIR}/ios-arm64/crsqlite.framework/crsqlite"
  install_name_tool -id "@rpath/crsqlite.framework/crsqlite" "${BUILD_DIR}/ios-arm64/crsqlite.framework/crsqlite"

  printf "\n\n\t\t===================== create ios simulator framework =====================\n\n"
  mkdir -p "${BUILD_DIR}/ios-arm64_x86_64-simulator/crsqlite.framework"
  echo "${plist}" > "${BUILD_DIR}/ios-arm64_x86_64-simulator/crsqlite.framework/Info.plist"
  cp -p "./dist-ios-sim/crsqlite-universal-ios-sim.dylib" "${BUILD_DIR}/ios-arm64_x86_64-simulator/crsqlite.framework/crsqlite"
  install_name_tool -id "@rpath/crsqlite.framework/crsqlite" "${BUILD_DIR}/ios-arm64_x86_64-simulator/crsqlite.framework/crsqlite"

  printf "\n\n\t\t===================== create ios xcframework =====================\n\n"
  rm -rf "${BUILD_DIR}/crsqlite.xcframework"
  xcodebuild -create-xcframework -framework "${BUILD_DIR}/ios-arm64/crsqlite.framework" -framework "${BUILD_DIR}/ios-arm64_x86_64-simulator/crsqlite.framework" -output "${BUILD_DIR}/crsqlite.xcframework"

  mkdir -p ${DIST_PACKAGE_DIR}
  cp -Rf "${BUILD_DIR}/crsqlite.xcframework" "${DIST_PACKAGE_DIR}/crsqlite.xcframework"
  cd ${DIST_PACKAGE_DIR}
  tar -czvf crsqlite-ios-dylib.xcframework.tar.gz crsqlite.xcframework
  rm -rf ${BUILD_DIR}
}

# Make all the non-simulator libs
# Package into a universal ios lib
mkdir -p ./dist-ios

# TODO: fix things up to not require a clean before each target.
make clean
export IOS_TARGET=aarch64-apple-ios; make loadable
cp ./dist/crsqlite.dylib ./dist-ios/crsqlite-aarch64-apple-ios.dylib

mkdir -p ./dist-ios-sim

make clean
export IOS_TARGET=aarch64-apple-ios-sim; make loadable
cp ./dist/crsqlite.dylib ./dist-ios-sim/crsqlite-aarch64-apple-ios-sim.dylib

make clean
export IOS_TARGET=x86_64-apple-ios; make loadable
cp ./dist/crsqlite.dylib ./dist-ios-sim/crsqlite-x86_64-apple-ios-sim.dylib

cd ./dist-ios-sim
lipo crsqlite-aarch64-apple-ios-sim.dylib crsqlite-x86_64-apple-ios-sim.dylib -create -output crsqlite-universal-ios-sim.dylib
cd ..

createXcframework

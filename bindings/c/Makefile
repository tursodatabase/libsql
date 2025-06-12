OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm
ARCHS_IOS = aarch64-apple-ios aarch64-apple-ios-sim
ARCHS_ANDROID = aarch64-linux-android armv7-linux-androideabi x86_64-linux-android i686-linux-android
LIB = libsql_experimental.a
HEADER = libsql.h
XCFRAMEWORK = libsql.xcframework

# Set LIBSQL_PATH to the default path if not provided
LIBSQL_EXPERIMENTAL_PATH ?= ../../target/release/libsql_experimental.a

ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all $(ARCHS_IOS) ios $(ARCHS_ANDROID) android

all: example

example: example.c
	$(CC) -o $@ $(CFLAGS) $< $(LIBSQL_EXPERIMENTAL_PATH) $(LDFLAGS)

android: $(ARCHS_ANDROID)
	rm -rf generated
	mkdir -p generated/jniLibs
	mkdir -p generated/jniLibs/arm64-v8a
	mkdir -p generated/jniLibs/armeabi-v7a
	mkdir -p generated/jniLibs/x86_64
	mkdir -p generated/jniLibs/x86

	cp ../../target/aarch64-linux-android/release/$(LIB) generated/jniLibs/arm64-v8a/$(LIB)
	cp ../../target/armv7-linux-androideabi/release/$(LIB) generated/jniLibs/armeabi-v7a/$(LIB)
	cp ../../target/x86_64-linux-android/release/$(LIB) generated/jniLibs/x86_64/$(LIB)
	cp ../../target/i686-linux-android/release/$(LIB) generated/jniLibs/x86/$(LIB)

$(ARCHS_ANDROID): %: 
	cargo ndk --target $@ --platform 31 build --release

ios: $(XCFRAMEWORK)

$(ARCHS_IOS): %:
	cargo build  --release --target $@

x86_64-apple-ios:
	SDKROOT=$$(xcrun --sdk iphonesimulator --show-sdk-path) \
	CFLAGS_x86_64_apple_ios="-target x86_64-apple-ios-simulator" \
	CXXFLAGS_x86_64_apple_ios="-target x86_64-apple-ios-simulator" \
	CC_x86_64_apple_ios=$$(xcrun --sdk iphonesimulator --find clang) \
	CXX_x86_64_apple_ios=$$(xcrun --sdk iphonesimulator --find clang++) \
	CMAKE_SYSTEM_NAME=iOS \
	CMAKE_OSX_SYSROOT=$$(xcrun --sdk iphonesimulator --show-sdk-path) \
	cargo build --release --target x86_64-apple-ios

$(XCFRAMEWORK): $(ARCHS_IOS) x86_64-apple-ios
	rm -rf generated
	mkdir -p generated/simulator_fat
	rm -rf $@
	lipo -create $(wildcard ../../target/x86_64-apple-ios/release/$(LIB)) $(wildcard ../../target/aarch64-apple-ios-sim/release/$(LIB)) -output generated/simulator_fat/$(LIB)
	xcodebuild -create-xcframework -library $(wildcard ../../target/aarch64-apple-ios/release/$(LIB)) -headers include -library generated/simulator_fat/$(LIB) -headers include -output $@

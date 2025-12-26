OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm
ARCHS_IOS = aarch64-apple-ios aarch64-apple-ios-sim
ARCHS_ANDROID = aarch64-linux-android armv7-linux-androideabi x86_64-linux-android i686-linux-android
IOS_LIB_FILE = libsql_experimental.dylib
IOS_FINAL_LIB_FILE = libsql_experimental
LIB_FILE = libsql_experimental.so
XCFRAMEWORK = libsql_experimental.xcframework
HEADER = libsql.h

ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all $(ARCHS_IOS) ios $(ARCHS_ANDROID) android

all: example

example: example.c
	$(CC) -o $@ $(CFLAGS) $< $(LIBSQL_EXPERIMENTAL_PATH) $(LDFLAGS)

notify:
	osascript -e 'display notification "Build completed" with title "libsql"'
	echo "🟢 Build Completed!"

lint:
	cargo clippy --all-features --all-targets -- --no-deps -D warnings

fix:
	cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged
	cargo fmt --all

nuke:
	rm -rf ./target

ios: clean ios-build package-dylibs notify

clean:
	rm -rf generated/$(XCFRAMEWORK)

package-dylibs:
	mkdir -p generated
# Delete old files
	rm -rf generated/libsql*.xcframework
	
# Reset the .xcframeworks from the templates
	cp -r templates/libsql*.xcframework generated

# Pack device dylibs
	cp ../../target/aarch64-apple-ios/release/$(IOS_LIB_FILE) generated/$(IOS_FINAL_LIB_FILE)

	cp generated/$(IOS_FINAL_LIB_FILE) generated/libsql_experimental.xcframework/ios-arm64/libsql_experimental.framework/
	install_name_tool -id @rpath/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE) generated/libsql_experimental.xcframework/ios-arm64/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE)
	codesign -f -s - --identifier com.turso.libsql-experimental generated/libsql_experimental.xcframework/ios-arm64/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE)
	
	mkdir -p generated/libsql_experimental.xcframework/ios-arm64/libsql_experimental.framework/Headers
	cp include/$(HEADER) generated/libsql_experimental.xcframework/ios-arm64/libsql_experimental.framework/Headers/

# Pack simulator dylibs
	cp ../../target/aarch64-apple-ios-sim/release/$(IOS_LIB_FILE) generated/$(IOS_FINAL_LIB_FILE)

	cp generated/$(IOS_FINAL_LIB_FILE) generated/libsql_experimental.xcframework/ios-arm64-simulator/libsql_experimental.framework/
	install_name_tool -id @rpath/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE) generated/libsql_experimental.xcframework/ios-arm64-simulator/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE)
	codesign -f -s - --identifier com.turso.libsql-experimental generated/libsql_experimental.xcframework/ios-arm64-simulator/libsql_experimental.framework/$(IOS_FINAL_LIB_FILE)
	
	mkdir -p generated/libsql_experimental.xcframework/ios-arm64-simulator/libsql_experimental.framework/Headers
	cp include/$(HEADER) generated/libsql_experimental.xcframework/ios-arm64-simulator/libsql_experimental.framework/Headers/

ios-build:
	cargo build --release --target aarch64-apple-ios 
	cargo build --release --target aarch64-apple-ios-sim

android: $(ARCHS_ANDROID)
	rm -rf generated
	mkdir -p generated/jniLibs
	mkdir -p generated/jniLibs/arm64-v8a
	mkdir -p generated/jniLibs/armeabi-v7a
	mkdir -p generated/jniLibs/x86_64
	mkdir -p generated/jniLibs/x86
	mkdir -p generated/jniLibs/include

	cp ../../target/aarch64-linux-android/release/$(LIB_FILE) generated/jniLibs/arm64-v8a/$(LIB_FILE)
	cp ../../target/armv7-linux-androideabi/release/$(LIB_FILE) generated/jniLibs/armeabi-v7a/$(LIB_FILE)
	cp ../../target/x86_64-linux-android/release/$(LIB_FILE) generated/jniLibs/x86_64/$(LIB_FILE)
	cp ../../target/i686-linux-android/release/$(LIB_FILE) generated/jniLibs/x86/$(LIB_FILE)
	cp include/$(HEADER) generated/jniLibs/include/

$(ARCHS_ANDROID): %: 
	cargo ndk --target $@ --platform 31 build --release

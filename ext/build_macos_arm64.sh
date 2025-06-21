#!/usr/bin/env bash
#
# Simple librdkafka build script for macOS
# Usage: ./build-librdkafka-macos.sh
#
# Expected directory structure:
#   ext/build_macos.sh                    (this script)
#   ext/build-common.sh                   (shared functions)
#   dist/librdkafka-*.tar.gz              (librdkafka source tarball)
#   dist/patches/*.patch                  (optional Ruby-specific patches)
#
set -euo pipefail

# Source common functions and constants
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/build_common.sh"

# Platform-specific paths
DIST_DIR="$SCRIPT_DIR/../dist"
PATCHES_DIR="$DIST_DIR/patches"
BUILD_DIR="$(pwd)/build-tmp-macos"
DEPS_PREFIX="/tmp/macos-deps"

# macOS-specific dependency check
check_macos_dependencies() {
    log "Checking macOS build dependencies..."

    # Check for Xcode Command Line Tools
    if ! xcode-select -p &> /dev/null; then
        error "Xcode Command Line Tools not found. Install with: xcode-select --install"
    fi

    # Check for required tools (in addition to common ones)
    local missing_tools=()

    command -v gcc &> /dev/null || missing_tools+=("gcc")
    command -v clang &> /dev/null || missing_tools+=("clang")

    if [ ${#missing_tools[@]} -gt 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
    fi

    log "✅ All required tools found"

    # Show system info
    log "Build environment:"
    log "  - macOS version: $(sw_vers -productVersion)"
    log "  - Architecture: $(uname -m)"
    log "  - Xcode tools: $(xcode-select -p)"
}

# macOS-specific compiler setup
setup_macos_compiler() {
    local arch="$1"

    # Get the proper macOS SDK path
    MACOS_SDK_PATH=$(xcrun --show-sdk-path)
    log "Using macOS SDK: $MACOS_SDK_PATH"

    # Set macOS-specific flags
    export CC="$(xcrun -find clang)"
    export CFLAGS="-fPIC -O2 -arch $arch -isysroot $MACOS_SDK_PATH"
    export CXXFLAGS="-fPIC -O2 -arch $arch -isysroot $MACOS_SDK_PATH"
    export CPPFLAGS="-isysroot $MACOS_SDK_PATH"

    log "Applied $arch specific flags"
}

# Build static OpenSSL for macOS
build_openssl_macos() {
    local arch="$1"
    local openssl_prefix="$2"
    local openssl_dir="$3"

    cd "$openssl_dir"

    if [ ! -f "$openssl_prefix/lib/libssl.a" ]; then
        log "Configuring and building static OpenSSL..."
        make clean 2>/dev/null || true

        setup_macos_compiler "$arch"

        # Configure OpenSSL for macOS
        if [ "$arch" = "arm64" ]; then
            ./Configure darwin64-arm64-cc \
                no-shared \
                no-dso \
                --prefix="$openssl_prefix" \
                --openssldir="$openssl_prefix/ssl"
        else
            ./Configure darwin64-x86_64-cc \
                no-shared \
                no-dso \
                --prefix="$openssl_prefix" \
                --openssldir="$openssl_prefix/ssl"
        fi

        make -j$(get_cpu_count)
        make install

        # Verify the build
        if [ ! -f "$openssl_prefix/lib/libssl.a" ] || [ ! -f "$openssl_prefix/lib/libcrypto.a" ]; then
            error "Failed to build static OpenSSL"
        fi

        log "✅ Static OpenSSL built successfully at $openssl_prefix"
    else
        log "Static OpenSSL already built, skipping..."
    fi
}

# Build static Cyrus SASL for macOS
build_sasl_macos() {
    local arch="$1"
    local sasl_prefix="$2"
    local sasl_dir="$3"
    local openssl_prefix="$4"

    cd "$sasl_dir"

    if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
        log "Configuring and building static Cyrus SASL..."
        make clean 2>/dev/null || true

        setup_macos_compiler "$arch"
        export CPPFLAGS="$CPPFLAGS -I$openssl_prefix/include"
        export LDFLAGS="-L$openssl_prefix/lib"

        # Configure SASL with minimal features for Kafka
        ./configure \
            --disable-shared \
            --enable-static \
            --prefix="$sasl_prefix" \
            --without-dblib \
            --disable-gdbm \
            --disable-macos-framework \
            --disable-sample \
            --disable-obsolete_cram_attr \
            --disable-obsolete_digest_attr \
            --with-openssl="$openssl_prefix"

        make -j$(get_cpu_count)
        make install

        # Verify the build
        if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
            error "Failed to build static Cyrus SASL"
        fi

        log "✅ Static Cyrus SASL built successfully at $sasl_prefix"
    else
        log "Static Cyrus SASL already built, skipping..."
    fi
}

# Build generic static library for macOS
build_static_lib_macos() {
    local lib_name="$1"
    local arch="$2"
    local prefix="$3"
    local source_dir="$4"
    local configure_args="$5"

    cd "$source_dir"

    local lib_file="$prefix/lib/lib${lib_name}.a"
    if [ ! -f "$lib_file" ]; then
        log "Configuring and building static $lib_name..."
        make clean 2>/dev/null || true

        setup_macos_compiler "$arch"

        # Run configure with provided arguments
        eval "./configure --prefix=\"$prefix\" $configure_args"

        make -j$(get_cpu_count)
        make install

        # Verify the build
        if [ ! -f "$lib_file" ]; then
            error "Failed to build static $lib_name"
        fi

        log "✅ Static $lib_name built successfully at $prefix"
    else
        log "Static $lib_name already built, skipping..."
    fi
}

# Check common and macOS-specific dependencies
check_common_dependencies
check_macos_dependencies

# Auto-detect librdkafka tarball
LIBRDKAFKA_TARBALL=$(find_librdkafka_tarball "$DIST_DIR")
LIBRDKAFKA_VERSION=$(basename "$LIBRDKAFKA_TARBALL" .tar.gz)
log "Version detected: $LIBRDKAFKA_VERSION"

# Find patches
PATCHES_FOUND=()
find_patches "$PATCHES_DIR" PATCHES_FOUND

# Detect architecture early since we need it for dependency building
ARCH=$(uname -m)
log "Detected architecture: $ARCH"

log "Building librdkafka for macOS"
log "librdkafka source: $LIBRDKAFKA_TARBALL"
log "Build directory: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Build static OpenSSL first (other deps might need it)
log "Building static OpenSSL $OPENSSL_VERSION..."
OPENSSL_PREFIX="$DEPS_PREFIX/static-openssl-$OPENSSL_VERSION"
OPENSSL_TARBALL="openssl-$OPENSSL_VERSION.tar.gz"
OPENSSL_DIR="openssl-$OPENSSL_VERSION"

secure_download "$(get_openssl_url)" "$OPENSSL_TARBALL"
extract_if_needed "$OPENSSL_TARBALL" "$OPENSSL_DIR"
build_openssl_macos "$ARCH" "$OPENSSL_PREFIX" "$OPENSSL_DIR"

cd "$BUILD_DIR"

# Build static Cyrus SASL (after OpenSSL since it might need crypto functions)
log "Building static Cyrus SASL $CYRUS_SASL_VERSION..."
SASL_PREFIX="$DEPS_PREFIX/static-sasl-$CYRUS_SASL_VERSION"
SASL_TARBALL="cyrus-sasl-$CYRUS_SASL_VERSION.tar.gz"
SASL_DIR="cyrus-sasl-$CYRUS_SASL_VERSION"

secure_download "$(get_sasl_url)" "$SASL_TARBALL"
extract_if_needed "$SASL_TARBALL" "$SASL_DIR"
build_sasl_macos "$ARCH" "$SASL_PREFIX" "$SASL_DIR" "$OPENSSL_PREFIX"

cd "$BUILD_DIR"

# Build static ZStd
log "Building static ZStd $ZSTD_VERSION..."
ZSTD_PREFIX="$DEPS_PREFIX/static-zstd-$ZSTD_VERSION"
ZSTD_TARBALL="zstd-$ZSTD_VERSION.tar.gz"
ZSTD_DIR="zstd-$ZSTD_VERSION"

secure_download "$(get_zstd_url)" "$ZSTD_TARBALL"
extract_if_needed "$ZSTD_TARBALL" "$ZSTD_DIR"
cd "$ZSTD_DIR"

if [ ! -f "$ZSTD_PREFIX/lib/libzstd.a" ]; then
    log "Configuring and building static ZStd..."
    make clean 2>/dev/null || true

    setup_macos_compiler "$ARCH"

    # Build static library using ZStd's Makefile
    make lib-mt CFLAGS="$CFLAGS" PREFIX="$ZSTD_PREFIX" -j$(get_cpu_count)
    make install PREFIX="$ZSTD_PREFIX"

    # Verify the build
    if [ ! -f "$ZSTD_PREFIX/lib/libzstd.a" ]; then
        error "Failed to build static ZStd"
    fi

    log "✅ Static ZStd built successfully at $ZSTD_PREFIX"
else
    log "Static ZStd already built, skipping..."
fi

cd "$BUILD_DIR"

# Build static zlib
log "Building static zlib $ZLIB_VERSION..."
ZLIB_PREFIX="$DEPS_PREFIX/static-zlib-$ZLIB_VERSION"
ZLIB_TARBALL="zlib-$ZLIB_VERSION.tar.gz"
ZLIB_DIR="zlib-$ZLIB_VERSION"

secure_download "$(get_zlib_url)" "$ZLIB_TARBALL"
extract_if_needed "$ZLIB_TARBALL" "$ZLIB_DIR"
build_static_lib_macos "z" "$ARCH" "$ZLIB_PREFIX" "$ZLIB_DIR" "--static"

cd "$BUILD_DIR"

# Extract librdkafka
log "Extracting librdkafka..."
tar xzf "$LIBRDKAFKA_TARBALL"
cd "$LIBRDKAFKA_VERSION"

# Fix permissions and apply patches
fix_configure_permissions
apply_patches PATCHES_FOUND

# Set compiler flags for librdkafka
setup_macos_compiler "$ARCH"

# Configure librdkafka with static dependencies
log "Configuring librdkafka with static dependencies..."

# Use our static libraries instead of system versions
export CPPFLAGS="$CPPFLAGS -I$OPENSSL_PREFIX/include -I$SASL_PREFIX/include -I$ZLIB_PREFIX/include -I$ZSTD_PREFIX/include"
export LDFLAGS="-L$OPENSSL_PREFIX/lib -L$SASL_PREFIX/lib -L$ZLIB_PREFIX/lib -L$ZSTD_PREFIX/lib"

if [ -f configure ]; then
    log "Using standard configure (autotools)"
    ./configure \
        --enable-static \
        --enable-shared \
        --disable-lz4-ext \
        --enable-sasl
else
    error "No configure script found"
fi

# Build librdkafka
log "Compiling librdkafka..."
make clean || true
make -j$(get_cpu_count)

# Verify build products exist
if [ ! -f src/librdkafka.a ]; then
    error "librdkafka.a not found after build"
fi

if [ ! -f src/librdkafka.1.dylib ]; then
    error "librdkafka.dylib not found after build"
fi

log "librdkafka built successfully"

# Create self-contained dylib (equivalent to Linux gcc -shared step)
log "Creating self-contained librdkafka.dylib..."

# Create self-contained shared library by linking all static dependencies
# This is the macOS equivalent of your Linux gcc -shared command
clang -dynamiclib -fPIC \
    -Wl,-force_load,src/librdkafka.a \
    -Wl,-force_load,"$SASL_PREFIX/lib/libsasl2.a" \
    -Wl,-force_load,"$OPENSSL_PREFIX/lib/libssl.a" \
    -Wl,-force_load,"$OPENSSL_PREFIX/lib/libcrypto.a" \
    -Wl,-force_load,"$ZLIB_PREFIX/lib/libz.a" \
    -Wl,-force_load,"$ZSTD_PREFIX/lib/libzstd.a" \
    -o librdkafka.dylib \
    -lpthread -lm -lc -arch $ARCH \
    -install_name @rpath/librdkafka.dylib \
    -Wl,-undefined,dynamic_lookup

if [ ! -f librdkafka.dylib ]; then
    error "Failed to create self-contained librdkafka.dylib"
fi

log "✅ Self-contained librdkafka.dylib created successfully"

# Verify the self-contained build
log "Verifying self-contained build..."
file librdkafka.dylib

log "Checking dependencies with otool (should only show system libraries):"
otool -L librdkafka.dylib

# Check for external dependencies that shouldn't be there (strict like Linux version)
log "Checking for external dependencies (should only show system libraries):"
EXTERNAL_DEPS=$(otool -L librdkafka.dylib | grep -v "librdkafka.dylib" | grep -v "/usr/lib/" | grep -v "/System/Library/" | grep -v "@rpath" || true)
if [ -n "$EXTERNAL_DEPS" ]; then
    error "Found external dependencies - library is not self-contained: $EXTERNAL_DEPS"
else
    log "✅ No external dependencies found - library is self-contained!"
fi

log "Checking exported symbols:"
# Avoid SIGPIPE by not using head in a pipe
nm -gU librdkafka.dylib > /tmp/symbols.txt 2>/dev/null || true
if [ -f /tmp/symbols.txt ]; then
    head -10 /tmp/symbols.txt
    rm -f /tmp/symbols.txt
else
    log "Could not extract symbols (this is normal)"
fi

# Force output flush and add small delay
sync
sleep 1

# Copy to output directory
OUTPUT_DIR="$SCRIPT_DIR"
cp librdkafka.dylib "$OUTPUT_DIR/"
cp src/librdkafka.a "$OUTPUT_DIR/"

log "Build artifacts copied to: $OUTPUT_DIR/"
log "  - librdkafka.dylib (shared library)"
log "  - librdkafka.a (static library)"

# Force another flush
sync
sleep 1

# Print summaries
print_security_summary

# Enhanced summary for macOS
sync
echo ""
echo "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉"
log "Build completed successfully!"
log "📦 Self-contained librdkafka built for macOS $ARCH:"
log "   ✅ Static library: librdkafka.a"
log "   ✅ Self-contained dylib: librdkafka.dylib (with bundled dependencies)"
log "   ✅ Static OpenSSL: $OPENSSL_VERSION (SSL/TLS support) - bundled"
log "   ✅ Static Cyrus SASL: $CYRUS_SASL_VERSION (authentication for AWS MSK) - bundled"
log "   ✅ Static zlib: $ZLIB_VERSION (compression) - bundled"
log "   ✅ Static ZStd: $ZSTD_VERSION (high-performance compression) - bundled"
log ""
log "🎯 Ready for deployment on macOS systems"
log "☁️  Compatible with AWS MSK and other secured Kafka clusters"
log "🔐 Supply chain security: All dependencies cryptographically verified"
log "📦 Self-contained: Ready for Ruby FFI distribution"
log ""
log "Location: $OUTPUT_DIR/librdkafka.dylib"
echo "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉"

# Force final flush
sync

# Cleanup
cleanup_build_dir "$BUILD_DIR"

# Reset environment variables
unset CFLAGS CXXFLAGS CPPFLAGS LDFLAGS

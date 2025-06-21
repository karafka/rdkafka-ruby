#!/usr/bin/env bash
#
# Build self-contained librdkafka.so for Linux x86_64 with checksum verification
# Usage: ./build-librdkafka-linux.sh
#
# Expected directory structure:
#   ext/build_linux_x86_64.sh            (this script)
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
BUILD_DIR="$(pwd)/build-tmp"
DEPS_PREFIX="/tmp"

# Check common dependencies
check_common_dependencies

# Linux-specific dependency check
log "Checking Linux-specific build dependencies..."
command -v gcc &> /dev/null || error "gcc not found. Install with: apt-get install build-essential"

# Auto-detect librdkafka tarball
log "Looking for librdkafka tarball in $DIST_DIR..."
LIBRDKAFKA_TARBALL=$(find_librdkafka_tarball "$DIST_DIR")
log "Found librdkafka tarball: $LIBRDKAFKA_TARBALL"

# Verify librdkafka tarball checksum if available
verify_librdkafka_checksum "$LIBRDKAFKA_TARBALL"

# Find patches
PATCHES_FOUND=()
find_patches "$PATCHES_DIR" PATCHES_FOUND

security_log "Starting secure build with checksum verification enabled"
log "Building self-contained librdkafka.so for Linux x86_64"
log "Dependencies to build:"
log "  - OpenSSL: $OPENSSL_VERSION"
log "  - Cyrus SASL: $CYRUS_SASL_VERSION"
log "  - MIT Kerberos: $KRB5_VERSION"
log "  - zlib: $ZLIB_VERSION"
log "  - ZStd: $ZSTD_VERSION"
log "librdkafka source: $LIBRDKAFKA_TARBALL"
log "Build directory: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Build OpenSSL
log "Building OpenSSL $OPENSSL_VERSION..."
OPENSSL_PREFIX="$DEPS_PREFIX/static-openssl-$OPENSSL_VERSION"
OPENSSL_TARBALL="openssl-$OPENSSL_VERSION.tar.gz"
OPENSSL_DIR="openssl-$OPENSSL_VERSION"

secure_download "$(get_openssl_url)" "$OPENSSL_TARBALL"
extract_if_needed "$OPENSSL_TARBALL" "$OPENSSL_DIR"
cd "$OPENSSL_DIR"

# Check if OpenSSL lib directory exists (lib or lib64)
if [ ! -f "$OPENSSL_PREFIX/lib/libssl.a" ] && [ ! -f "$OPENSSL_PREFIX/lib64/libssl.a" ]; then
    log "Configuring and building OpenSSL..."
    export CFLAGS="-fPIC"
    ./Configure linux-x86_64 \
        no-shared \
        no-dso \
        --prefix="$OPENSSL_PREFIX"
    make clean || true
    make -j$(get_cpu_count)
    make install
    unset CFLAGS
    log "OpenSSL built successfully"
else
    log "OpenSSL already built, skipping..."
fi

# Determine OpenSSL lib directory
if [ -f "$OPENSSL_PREFIX/lib64/libssl.a" ]; then
    OPENSSL_LIB_DIR="$OPENSSL_PREFIX/lib64"
else
    OPENSSL_LIB_DIR="$OPENSSL_PREFIX/lib"
fi
log "OpenSSL libraries in: $OPENSSL_LIB_DIR"

cd "$BUILD_DIR"

# Build MIT Kerberos (krb5)
log "Building MIT Kerberos $KRB5_VERSION..."
KRB5_PREFIX="$DEPS_PREFIX/static-krb5-$KRB5_VERSION"
KRB5_TARBALL="krb5-$KRB5_VERSION.tar.gz"
KRB5_DIR="krb5-$KRB5_VERSION"

secure_download "$(get_krb5_url)" "$KRB5_TARBALL"
extract_if_needed "$KRB5_TARBALL" "$KRB5_DIR"
cd "$KRB5_DIR/src"

if [ ! -f "$KRB5_PREFIX/lib/libgssapi_krb5.a" ]; then
    log "Configuring and building MIT Kerberos..."
    make clean 2>/dev/null || true
    ./configure --disable-shared --enable-static --prefix="$KRB5_PREFIX" \
        --without-ldap --without-tcl --without-keyutils \
        --disable-rpath --without-system-verto \
        CFLAGS="-fPIC" CXXFLAGS="-fPIC"

    # Build everything except the problematic kadmin tools
    log "Building Kerberos (will ignore kadmin build failures)..."
    make -j$(get_cpu_count) || {
        log "Full build failed (expected due to kadmin), continuing with libraries..."
        # The libraries should be built even if kadmin fails
        true
    }

    # Install what was successfully built
    make install || {
        log "Full install failed, installing individual components..."
        # Try to install the core libraries manually
        make install-mkdirs 2>/dev/null || true
        make -C util install 2>/dev/null || true
        make -C lib install 2>/dev/null || true
        make -C plugins/kdb/db2 install 2>/dev/null || true
    }

    # Verify we got the essential libraries
    if [ ! -f "$KRB5_PREFIX/lib/libgssapi_krb5.a" ]; then
        error "Failed to build essential Kerberos libraries"
    fi

    log "MIT Kerberos libraries built successfully"
else
    log "MIT Kerberos already built, skipping..."
fi

cd "$BUILD_DIR"

# Build SASL
log "Building Cyrus SASL $CYRUS_SASL_VERSION..."
SASL_PREFIX="$DEPS_PREFIX/static-sasl-$CYRUS_SASL_VERSION"
SASL_TARBALL="cyrus-sasl-$CYRUS_SASL_VERSION.tar.gz"
SASL_DIR="cyrus-sasl-$CYRUS_SASL_VERSION"

secure_download "$(get_sasl_url)" "$SASL_TARBALL"
extract_if_needed "$SASL_TARBALL" "$SASL_DIR"
cd "$SASL_DIR"

if [ ! -f "$SASL_PREFIX/lib/libsasl2.a" ]; then
    log "Configuring and building SASL..."
    make clean 2>/dev/null || true
    ./configure --disable-shared --enable-static --prefix="$SASL_PREFIX" \
        --without-dblib --disable-gdbm \
        --enable-gssapi="$KRB5_PREFIX" \
        CFLAGS="-fPIC" CXXFLAGS="-fPIC" \
        CPPFLAGS="-I$KRB5_PREFIX/include" \
        LDFLAGS="-L$KRB5_PREFIX/lib"
    make -j$(get_cpu_count)
    make install
    log "SASL built successfully"
else
    log "SASL already built, skipping..."
fi

cd "$BUILD_DIR"

# Build zlib
log "Building zlib $ZLIB_VERSION..."
ZLIB_PREFIX="$DEPS_PREFIX/static-zlib-$ZLIB_VERSION"
ZLIB_TARBALL="zlib-$ZLIB_VERSION.tar.gz"
ZLIB_DIR="zlib-$ZLIB_VERSION"

secure_download "$(get_zlib_url)" "$ZLIB_TARBALL"
extract_if_needed "$ZLIB_TARBALL" "$ZLIB_DIR"
cd "$ZLIB_DIR"

if [ ! -f "$ZLIB_PREFIX/lib/libz.a" ]; then
    log "Configuring and building zlib..."
    make clean 2>/dev/null || true
    export CFLAGS="-fPIC"
    ./configure --prefix="$ZLIB_PREFIX" --static
    make -j$(get_cpu_count)
    make install
    unset CFLAGS
    log "zlib built successfully"
else
    log "zlib already built, skipping..."
fi

cd "$BUILD_DIR"

# Build ZStd
log "Building ZStd $ZSTD_VERSION..."
ZSTD_PREFIX="$DEPS_PREFIX/static-zstd-$ZSTD_VERSION"
ZSTD_TARBALL="zstd-$ZSTD_VERSION.tar.gz"
ZSTD_DIR="zstd-$ZSTD_VERSION"

secure_download "$(get_zstd_url)" "$ZSTD_TARBALL"
extract_if_needed "$ZSTD_TARBALL" "$ZSTD_DIR"
cd "$ZSTD_DIR"

if [ ! -f "$ZSTD_PREFIX/lib/libzstd.a" ]; then
    log "Building ZStd..."
    make clean 2>/dev/null || true
    make lib-mt CFLAGS="-fPIC" PREFIX="$ZSTD_PREFIX" -j$(get_cpu_count)
    # Use standard install target - install-pc may not exist in all versions
    make install PREFIX="$ZSTD_PREFIX"
    log "ZStd built successfully"
else
    log "ZStd already built, skipping..."
fi

cd "$BUILD_DIR"

# Extract and patch librdkafka
log "Extracting librdkafka..."
tar xzf "$LIBRDKAFKA_TARBALL"
cd "librdkafka-$LIBRDKAFKA_VERSION"

# Fix permissions and apply patches
fix_configure_permissions
apply_patches PATCHES_FOUND

# Configure librdkafka
log "Configuring librdkafka..."

if [ -f configure ]; then
    log "Using standard configure (autotools)"
    # Export environment variables for configure to pick up
    export CPPFLAGS="-I$KRB5_PREFIX/include"
    export LDFLAGS="-L$KRB5_PREFIX/lib"

    ./configure --enable-static --disable-shared --disable-curl \
        --enable-gssapi

    # Clean up environment variables
    unset CPPFLAGS LDFLAGS
else
    error "No configure script found (checked: configure.self, configure)"
fi

# Build librdkafka
log "Compiling librdkafka..."
make clean || true
make -j$(get_cpu_count)

# Verify librdkafka.a exists
if [ ! -f src/librdkafka.a ]; then
    error "librdkafka.a not found after build"
fi

log "librdkafka.a built successfully"

# Create self-contained shared library
log "Creating self-contained librdkafka.so..."

gcc -shared -fPIC -Wl,--whole-archive src/librdkafka.a -Wl,--no-whole-archive \
    -o librdkafka.so \
    "$SASL_PREFIX/lib/libsasl2.a" \
    "$KRB5_PREFIX/lib/libgssapi_krb5.a" \
    "$KRB5_PREFIX/lib/libkrb5.a" \
    "$KRB5_PREFIX/lib/libk5crypto.a" \
    "$KRB5_PREFIX/lib/libcom_err.a" \
    "$KRB5_PREFIX/lib/libkrb5support.a" \
    "$OPENSSL_LIB_DIR/libssl.a" \
    "$OPENSSL_LIB_DIR/libcrypto.a" \
    "$ZLIB_PREFIX/lib/libz.a" \
    "$ZSTD_PREFIX/lib/libzstd.a" \
    -lpthread -lm -ldl -lresolv

if [ ! -f librdkafka.so ]; then
    error "Failed to create librdkafka.so"
fi

log "librdkafka.so created successfully"

# Verify the build
log "Verifying build..."
file librdkafka.so

log "Checking dependencies with ldd:"
ldd librdkafka.so

log "Checking for external dependencies (should only show system libraries):"
EXTERNAL_DEPS=$(nm -D librdkafka.so | grep " U " | grep -v "@GLIBC" || true)
if [ -n "$EXTERNAL_DEPS" ]; then
    error "Found external dependencies - library is not self-contained: $EXTERNAL_DEPS"
else
    log "✅ No external dependencies found - library is self-contained!"
fi

# Copy to output directory
OUTPUT_DIR="$SCRIPT_DIR"
cp librdkafka.so "$OUTPUT_DIR/"
log "librdkafka.so copied to: $OUTPUT_DIR/librdkafka.so"

# Print summaries
print_security_summary
print_build_summary "Linux" "x86_64" "$OUTPUT_DIR" "librdkafka.so"

# Cleanup
cleanup_build_dir "$BUILD_DIR"

#!/usr/bin/env bash
#
# Simple librdkafka build script for macOS with Kerberos support
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
                --openssldir="/etc/ssl" \
                --with-rand-seed=os \
                -DOPENSSL_NO_HEARTBEATS
        else
            ./Configure darwin64-x86_64-cc \
                no-shared \
                no-dso \
                --prefix="$openssl_prefix" \
                --openssldir="/etc/ssl" \
                --with-rand-seed=os \
                -DOPENSSL_NO_HEARTBEATS
        fi

        make -j$(get_cpu_count)
        make install_sw

        # Verify the build
        if [ ! -f "$openssl_prefix/lib/libssl.a" ] || [ ! -f "$openssl_prefix/lib/libcrypto.a" ]; then
            error "Failed to build static OpenSSL"
        fi

        log "✅ Static OpenSSL built successfully at $openssl_prefix"
    else
        log "Static OpenSSL already built, skipping..."
    fi
}

# Build static MIT Kerberos for macOS
build_krb5_macos() {
    local arch="$1"
    local krb5_prefix="$2"
    local krb5_dir="$3"

    cd "$krb5_dir/src"

    if [ ! -f "$krb5_prefix/lib/libgssapi_krb5.a" ]; then
        log "Configuring and building static MIT Kerberos..."
        make clean 2>/dev/null || true

        setup_macos_compiler "$arch"

        # Configure MIT Kerberos for macOS
        ./configure \
            --disable-shared \
            --enable-static \
            --prefix="$krb5_prefix" \
            --without-ldap \
            --without-tcl \
            --without-keyutils \
            --disable-rpath \
            --without-system-verto \
            --disable-thread-support \
            --disable-aesni

        # Build everything except the problematic kadmin tools (same as Linux)
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
        if [ ! -f "$krb5_prefix/lib/libgssapi_krb5.a" ]; then
            error "Failed to build essential Kerberos libraries"
        fi

        log "✅ Static MIT Kerberos built successfully at $krb5_prefix"
    else
        log "Static MIT Kerberos already built, skipping..."
    fi
}

# Build static Cyrus SASL for macOS with Kerberos support
build_sasl_macos() {
    local arch="$1"
    local sasl_prefix="$2"
    local sasl_dir="$3"
    local openssl_prefix="$4"
    local krb5_prefix="$5"

    cd "$sasl_dir"

    if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
        log "Configuring and building static Cyrus SASL with Kerberos support..."
        make clean 2>/dev/null || true

        setup_macos_compiler "$arch"
        export CPPFLAGS="$CPPFLAGS -I$openssl_prefix/include -I$krb5_prefix/include"
        export LDFLAGS="-L$openssl_prefix/lib -L$krb5_prefix/lib"

        # Configure SASL with Kerberos/GSSAPI support (now ENABLED)
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
            --enable-gssapi="$krb5_prefix" \
            --disable-krb4 \
            --with-openssl="$openssl_prefix"

        make -j$(get_cpu_count)
        make install

        # Verify the build
        if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
            error "Failed to build static Cyrus SASL"
        fi

        log "✅ Static Cyrus SASL with Kerberos support built successfully at $sasl_prefix"
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
log "Looking for librdkafka tarball in $DIST_DIR..."
LIBRDKAFKA_TARBALL=$(find_librdkafka_tarball "$DIST_DIR")
log "Found librdkafka tarball: $LIBRDKAFKA_TARBALL"

# Verify librdkafka tarball checksum if available
verify_librdkafka_checksum "$LIBRDKAFKA_TARBALL"

# Find patches
PATCHES_FOUND=()
find_patches "$PATCHES_DIR" PATCHES_FOUND

# Detect architecture early since we need it for dependency building
ARCH=$(uname -m)
log "Detected architecture: $ARCH"

security_log "Starting secure build with checksum verification enabled"
log "Building self-contained librdkafka for macOS with Kerberos support"
log "Dependencies to build:"
log "  - OpenSSL: $OPENSSL_VERSION"
log "  - Cyrus SASL: $CYRUS_SASL_VERSION (with Kerberos support)"
log "  - MIT Kerberos: $KRB5_VERSION"
log "  - zlib: $ZLIB_VERSION"
log "  - ZStd: $ZSTD_VERSION"
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

# Build static MIT Kerberos (before SASL since SASL needs it)
log "Building static MIT Kerberos $KRB5_VERSION..."
KRB5_PREFIX="$DEPS_PREFIX/static-krb5-$KRB5_VERSION"
KRB5_TARBALL="krb5-$KRB5_VERSION.tar.gz"
KRB5_DIR="krb5-$KRB5_VERSION"

secure_download "$(get_krb5_url)" "$KRB5_TARBALL"
extract_if_needed "$KRB5_TARBALL" "$KRB5_DIR"
build_krb5_macos "$ARCH" "$KRB5_PREFIX" "$KRB5_DIR"

cd "$BUILD_DIR"

# Build static Cyrus SASL (after OpenSSL and Kerberos since it needs both)
log "Building static Cyrus SASL $CYRUS_SASL_VERSION with Kerberos support..."
SASL_PREFIX="$DEPS_PREFIX/static-sasl-$CYRUS_SASL_VERSION"
SASL_TARBALL="cyrus-sasl-$CYRUS_SASL_VERSION.tar.gz"
SASL_DIR="cyrus-sasl-$CYRUS_SASL_VERSION"

secure_download "$(get_sasl_url)" "$SASL_TARBALL"
extract_if_needed "$SASL_TARBALL" "$SASL_DIR"
build_sasl_macos "$ARCH" "$SASL_PREFIX" "$SASL_DIR" "$OPENSSL_PREFIX" "$KRB5_PREFIX"

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

# Completely disable pkg-config to prevent Homebrew library detection
log "Disabling pkg-config to prevent Homebrew interference..."
export PKG_CONFIG=""
export PKG_CONFIG_PATH=""
export PKG_CONFIG_LIBDIR=""

# Create a dummy pkg-config that always fails
mkdir -p "$BUILD_DIR/no-pkg-config"
cat > "$BUILD_DIR/no-pkg-config/pkg-config" << 'EOF'
#!/bin/sh
# Dummy pkg-config that always fails to prevent Homebrew detection
exit 1
EOF
chmod +x "$BUILD_DIR/no-pkg-config/pkg-config"

# Put our dummy pkg-config first in PATH
export PATH="$BUILD_DIR/no-pkg-config:$PATH"

log "pkg-config disabled - configure will use manual library detection only"

# Extract librdkafka
log "Extracting librdkafka..."
tar xzf "$LIBRDKAFKA_TARBALL"
cd "librdkafka-$LIBRDKAFKA_VERSION"

# Fix permissions and apply patches
fix_configure_permissions
apply_patches PATCHES_FOUND

# Set compiler flags for librdkafka
setup_macos_compiler "$ARCH"

# Configure librdkafka with static dependencies INCLUDING Kerberos
log "Configuring librdkafka with static dependencies including Kerberos..."

# Tell configure that math functions don't need -lm on macOS
export ac_cv_lib_m_floor=yes
export ac_cv_lib_m_ceil=yes
export ac_cv_lib_m_sqrt=yes
export ac_cv_lib_m_pow=yes
export LIBS=""  # Clear any LIBS that might include -lm

# Use our static libraries instead of system versions (now including Kerberos)
export CPPFLAGS="$CPPFLAGS -I$OPENSSL_PREFIX/include -I$SASL_PREFIX/include -I$KRB5_PREFIX/include -I$ZLIB_PREFIX/include -I$ZSTD_PREFIX/include"
export LDFLAGS="-L$OPENSSL_PREFIX/lib -L$SASL_PREFIX/lib -L$KRB5_PREFIX/lib -L$ZLIB_PREFIX/lib -L$ZSTD_PREFIX/lib"

if [ -f configure ]; then
    log "Using mklove configure script"
    ./configure \
        --enable-static \
        --disable-shared \
        --disable-curl \
        --enable-gssapi
else
    error "No configure script found"
fi

# Fix system library path for linking
MACOS_SDK_PATH=$(xcrun --show-sdk-path)
export LDFLAGS="$LDFLAGS -L$MACOS_SDK_PATH/usr/lib"

# Build librdkafka
log "Compiling librdkafka..."
make clean || true

# Build with LIBS override, but ignore dylib build failures
make -j$(get_cpu_count) LIBS="" || {
    log "Build failed (expected - dylib linking issue), checking if static library was created..."
}

# Verify static library exists (this is what we actually need)
if [ ! -f src/librdkafka.a ]; then
    error "librdkafka.a not found after build"
fi

log "✅ Static librdkafka.a built successfully"

# Remove the dylib check since we're building our own
# Don't check for src/librdkafka.1.dylib

log "librdkafka built successfully - proceeding to create custom self-contained dylib"

# Create self-contained dylib with Kerberos libraries included
log "Creating self-contained librdkafka.dylib with Kerberos support..."

# Create self-contained shared library by linking all static dependencies (NOW INCLUDING KERBEROS)
# This is the macOS equivalent of your Linux gcc -shared command

# Write symbol export file (macOS equivalent of export.map)
cat > export_symbols.txt <<'EOF'
_rd_kafka_*
EOF

clang -dynamiclib -fPIC \
    -Wl,-force_load,src/librdkafka.a \
    -Wl,-force_load,"$SASL_PREFIX/lib/libsasl2.a" \
    -Wl,-force_load,"$KRB5_PREFIX/lib/libgssapi_krb5.a" \
    -Wl,-force_load,"$KRB5_PREFIX/lib/libkrb5.a" \
    -Wl,-force_load,"$KRB5_PREFIX/lib/libk5crypto.a" \
    -Wl,-force_load,"$KRB5_PREFIX/lib/libcom_err.a" \
    -Wl,-force_load,"$KRB5_PREFIX/lib/libkrb5support.a" \
    -Wl,-force_load,"$OPENSSL_PREFIX/lib/libssl.a" \
    -Wl,-force_load,"$OPENSSL_PREFIX/lib/libcrypto.a" \
    -Wl,-force_load,"$ZLIB_PREFIX/lib/libz.a" \
    -Wl,-force_load,"$ZSTD_PREFIX/lib/libzstd.a" \
    -o librdkafka.dylib \
    -lpthread -lc -arch $ARCH -lresolv \
    -framework GSS -framework Kerberos \
    -install_name @rpath/librdkafka.dylib \
    -Wl,-undefined,dynamic_lookup

if [ ! -f librdkafka.dylib ]; then
    error "Failed to create self-contained librdkafka.dylib"
fi

log "✅ Self-contained librdkafka.dylib with Kerberos support created successfully"

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

# Enhanced summary for macOS with Kerberos
sync
echo ""
echo "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉"
log "Build completed successfully!"
log "📦 Self-contained librdkafka built for macOS $ARCH with Kerberos support:"
log "   ✅ Static library: librdkafka.a"
log "   ✅ Self-contained dylib: librdkafka.dylib (with bundled dependencies)"
log "   ✅ Static OpenSSL: $OPENSSL_VERSION (SSL/TLS support) - bundled"
log "   ✅ Static Cyrus SASL: $CYRUS_SASL_VERSION (authentication for AWS MSK) - bundled"
log "   ✅ Static MIT Kerberos: $KRB5_VERSION (GSSAPI/Kerberos authentication) - bundled"
log "   ✅ Static zlib: $ZLIB_VERSION (compression) - bundled"
log "   ✅ Static ZStd: $ZSTD_VERSION (high-performance compression) - bundled"
log ""
log "🎯 Ready for deployment on macOS systems"
log "☁️  Compatible with AWS MSK and other secured Kafka clusters"
log "🔐 Supply chain security: All dependencies cryptographically verified"
log "📦 Self-contained: Ready for Ruby FFI distribution"
log "🔑 Kerberos/GSSAPI support: Full feature parity with Linux build"
log ""
log "Location: $OUTPUT_DIR/librdkafka.dylib"
echo "🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉"

# Force final flush
sync

# Cleanup
cleanup_build_dir "$BUILD_DIR"

# Reset environment variables
unset CFLAGS CXXFLAGS CPPFLAGS LDFLAGS

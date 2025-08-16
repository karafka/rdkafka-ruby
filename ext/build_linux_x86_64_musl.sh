#!/usr/bin/env bash
#
# Build self-contained librdkafka.so for Linux x86_64 musl with checksum verification
# Usage: ./build-librdkafka-linux-musl.sh
#
# Expected directory structure:
#   ext/build_linux_x86_64_musl.sh       (this script)
#   ext/build_common.sh                   (shared functions)
#   dist/librdkafka-*.tar.gz              (librdkafka source tarball)
#   dist/patches/*.patch                  (optional Ruby-specific patches)
#
set -euo pipefail

# Source common functions and constants
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/build_common.sh"

# Override secure_download for musl/Alpine compatibility
secure_download() {
    local url="$1"
    local filename="$2"

    if [ -f "$filename" ]; then
        log "File $filename already exists, verifying checksum..."
        verify_checksum "$filename"
        return 0
    fi

    log "Downloading $filename from $url..."

    # Use platform-appropriate download command
    if command -v wget &> /dev/null; then
        # Check if we have GNU wget or BusyBox wget
        if wget --help 2>&1 | grep -q "secure-protocol"; then
            # GNU wget - use security options
            if ! wget --secure-protocol=TLSv1_2 \
                     --https-only \
                     --timeout=30 \
                     --tries=3 \
                     --progress=bar \
                     "$url" \
                     -O "$filename"; then
                error "Failed to download $filename from $url"
            fi
        else
            # BusyBox wget (Alpine) - use basic options
            if ! wget -T 30 \
                     -t 3 \
                     -q \
                     "$url" \
                     -O "$filename"; then
                error "Failed to download $filename from $url"
            fi
        fi
    elif command -v curl &> /dev/null; then
        # curl with security options
        if ! curl -L \
                 --tlsv1.2 \
                 --connect-timeout 30 \
                 --max-time 300 \
                 --retry 3 \
                 --progress-bar \
                 "$url" \
                 -o "$filename"; then
            error "Failed to download $filename from $url"
        fi
    else
        error "No download utility found (tried wget, curl)"
    fi

    # Verify checksum immediately after download
    verify_checksum "$filename"
}

# Platform-specific paths
DIST_DIR="$SCRIPT_DIR/../dist"
PATCHES_DIR="$DIST_DIR/patches"
BUILD_DIR="$(pwd)/build-tmp-musl"
DEPS_PREFIX="/tmp/musl-deps"

# musl-specific dependency check
check_musl_dependencies() {
    log "Checking musl-specific build dependencies..."

    # Check if we're actually on musl
    if command -v ldd &> /dev/null && ldd --version 2>&1 | grep -q musl; then
        log "✅ musl libc detected"
    elif [ -f /lib/ld-musl-x86_64.so.1 ]; then
        log "✅ musl detected via loader presence"
    else
        warn "Not clearly detected as musl system. This script is optimized for musl-based distributions (Alpine, etc.)"
        warn "Continuing anyway, but glibc-specific optimizations may be used instead..."
    fi

    # Check for essential build tools
    local missing_tools=()

    command -v gcc &> /dev/null || missing_tools+=("gcc")
    command -v perl &> /dev/null || missing_tools+=("perl")
    command -v make &> /dev/null || missing_tools+=("make")
    command -v autoconf &> /dev/null || missing_tools+=("autoconf")
    command -v automake &> /dev/null || missing_tools+=("automake")
    command -v libtool &> /dev/null || missing_tools+=("libtool")
    command -v bison &> /dev/null || missing_tools+=("bison")
    command -v flex &> /dev/null || missing_tools+=("flex")

    # Check for system libraries that dependencies expect
    if [ ! -f /usr/include/sys/types.h ]; then
        missing_tools+=("musl-dev (for system headers)")
    fi

    # On Alpine, these packages provide the tools:
    # build-base: gcc, make, musl-dev, libc-dev, etc.
    # linux-headers: kernel headers for system calls
    # pkgconf: pkg-config for dependency detection
    # perl: required by OpenSSL and other build systems
    # autoconf, automake, libtool: GNU autotools for configure scripts
    # bison, flex: parser generators (needed by some dependencies)
    # file: for file type detection

    if [ ${#missing_tools[@]} -gt 0 ]; then
        error "Missing required tools: ${missing_tools[*]}

On Alpine Linux, install ALL required packages with:
    apk add build-base linux-headers pkgconf perl \\
            autoconf automake libtool bison flex file

Or install individually as needed."
    fi

    log "✅ All required musl build tools found"
}

# musl-specific compiler setup
setup_musl_compiler() {
    # musl-specific compiler flags
    export CC="gcc"
    export CFLAGS="-fPIC -O2 -static-libgcc"
    export CXXFLAGS="-fPIC -O2 -static-libgcc" 
    export CPPFLAGS=""
    export LDFLAGS="-static-libgcc"

    log "Applied musl-specific compiler flags"
}

# Build OpenSSL for musl
build_openssl_musl() {
    local openssl_prefix="$1"
    local openssl_dir="$2"

    cd "$openssl_dir"

    # Check both lib and lib64 directories for existing installation
    if [ ! -f "$openssl_prefix/lib/libssl.a" ] && [ ! -f "$openssl_prefix/lib64/libssl.a" ]; then
        log "Configuring and building OpenSSL for musl..."
        make clean 2>/dev/null || true

        setup_musl_compiler

        # Create the prefix directory first
        mkdir -p "$openssl_prefix"

        # Configure OpenSSL for musl (linux-x86_64 works fine)
        ./Configure linux-x86_64 \
            no-shared \
            no-dso \
            no-engine \
            --prefix="$openssl_prefix" \
            --openssldir="$openssl_prefix/ssl"

        make -j$(get_cpu_count)

        # Try the install and capture any errors
        log "Installing OpenSSL..."
        if ! make install_sw install_ssldirs 2>&1; then
            warn "install_sw failed, trying full install..."
            make install || {
                # If install fails, check if libraries were actually built
                if [ -f "libssl.a" ] && [ -f "libcrypto.a" ]; then
                    log "Install failed but libraries exist, copying manually..."
                    mkdir -p "$openssl_prefix/lib" "$openssl_prefix/include"
                    cp libssl.a libcrypto.a "$openssl_prefix/lib/"
                    cp -r include/openssl "$openssl_prefix/include/" 2>/dev/null || true
                else
                    error "OpenSSL build failed - no libraries found"
                fi
            }
        fi

        # Verify the build - check both possible lib locations
        if [ -f "$openssl_prefix/lib/libssl.a" ] || [ -f "$openssl_prefix/lib64/libssl.a" ]; then
            if [ -f "$openssl_prefix/lib64/libssl.a" ]; then
                log "OpenSSL installed to lib64 directory"
                # Create symlinks in lib for consistency
                mkdir -p "$openssl_prefix/lib"
                ln -sf ../lib64/libssl.a "$openssl_prefix/lib/libssl.a" 2>/dev/null || true
                ln -sf ../lib64/libcrypto.a "$openssl_prefix/lib/libcrypto.a" 2>/dev/null || true
                ln -sf ../lib64/pkgconfig "$openssl_prefix/lib/pkgconfig" 2>/dev/null || true
            fi
            log "✅ Static OpenSSL built successfully"
        else
            error "Failed to build static OpenSSL - libraries not found in lib or lib64"
        fi
    else
        log "Static OpenSSL already built, skipping..."
    fi
}

# Build MIT Kerberos for musl
build_krb5_musl() {
    local krb5_prefix="$1"
    local krb5_dir="$2"

    cd "$krb5_dir/src"

    if [ ! -f "$krb5_prefix/lib/libgssapi_krb5.a" ]; then
        log "Configuring and building MIT Kerberos for musl..."
        make clean 2>/dev/null || true

        setup_musl_compiler

        # musl-specific configuration for Kerberos
        # Disable some features that can be problematic on musl
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
            --disable-dns-for-realm \
            --without-readline

        # Build with limited parallelism to avoid musl-specific race conditions
        log "Building Kerberos (will ignore kadmin build failures)..."
        make -j$(( $(get_cpu_count) / 2 )) || {
            log "Full build failed (expected due to kadmin), continuing with libraries..."
            true
        }

        # Install what was successfully built
        make install || {
            log "Full install failed, installing individual components..."
            make install-mkdirs 2>/dev/null || true
            make -C util install 2>/dev/null || true
            make -C lib install 2>/dev/null || true
            make -C plugins/kdb/db2 install 2>/dev/null || true
        }

        # Verify we got the essential libraries
        if [ ! -f "$krb5_prefix/lib/libgssapi_krb5.a" ]; then
            error "Failed to build essential Kerberos libraries"
        fi

        log "✅ Static MIT Kerberos built successfully"
    else
        log "Static MIT Kerberos already built, skipping..."
    fi
}

# Build Cyrus SASL for musl with proper OpenSSL linking
build_sasl_musl() {
    local sasl_prefix="$1"
    local sasl_dir="$2"
    local krb5_prefix="$3"
    local openssl_prefix="$4"  # Add OpenSSL prefix parameter

    cd "$sasl_dir"

    if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
        log "Configuring and building Cyrus SASL for musl..."
        make clean 2>/dev/null || true

        # Apply comprehensive patches for missing time.h includes
        log "Applying comprehensive time.h patches..."

        # Create backups first
        cp lib/saslutil.c lib/saslutil.c.backup 2>/dev/null || true
        cp plugins/cram.c plugins/cram.c.backup 2>/dev/null || true

        # Use perl for more reliable in-place editing
        log "Patching lib/saslutil.c..."
        perl -i -pe 's/^#include "saslint\.h"$/#include "saslint.h"\n#include <time.h>/' lib/saslutil.c

        log "Patching plugins/cram.c..."
        perl -i -pe 's/^#include "plugin_common\.h"$/#include "plugin_common.h"\n#include <time.h>/' plugins/cram.c

        # Verify patches applied
        if ! grep -q "#include <time.h>" lib/saslutil.c; then
            log "Perl approach failed, using awk fallback for saslutil.c..."
            awk '/^#include "saslint\.h"$/ {print; print "#include <time.h>"; next} {print}' lib/saslutil.c.backup > lib/saslutil.c
        fi

        if ! grep -q "#include <time.h>" plugins/cram.c; then
            log "Perl approach failed, using awk fallback for cram.c..."
            awk '/^#include "plugin_common\.h"$/ {print; print "#include <time.h>"; next} {print}' plugins/cram.c.backup > plugins/cram.c
        fi

        # Clean up backup files
        rm -f lib/saslutil.c.backup plugins/cram.c.backup

        # Verify patches were applied
        if ! grep -q "#include <time.h>" lib/saslutil.c || ! grep -q "#include <time.h>" plugins/cram.c; then
            error "Failed to patch time.h includes - this is required for compilation"
        fi

        log "✅ time.h patches applied successfully"

        # Determine correct OpenSSL lib directory
        local openssl_lib_dir
        if [ -f "$openssl_prefix/lib64/libssl.a" ]; then
            openssl_lib_dir="$openssl_prefix/lib64"
            log "Using OpenSSL libraries from lib64"
        else
            openssl_lib_dir="$openssl_prefix/lib"
            log "Using OpenSSL libraries from lib"
        fi

        # Verify OpenSSL libraries exist
        if [ ! -f "$openssl_lib_dir/libssl.a" ] || [ ! -f "$openssl_lib_dir/libcrypto.a" ]; then
            error "OpenSSL static libraries not found in $openssl_lib_dir"
        fi

        setup_musl_compiler

        # Set comprehensive OpenSSL and Kerberos flags
        export CPPFLAGS="$CPPFLAGS -I$krb5_prefix/include -I$openssl_prefix/include"
        export LDFLAGS="$LDFLAGS -L$krb5_prefix/lib -L$openssl_lib_dir"
        export LIBS="-lssl -lcrypto -ldl -lpthread"

        # Also set PKG_CONFIG_PATH for better library detection
        export PKG_CONFIG_PATH="$openssl_lib_dir/pkgconfig:$krb5_prefix/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

        log "Environment variables set:"
        log "  CPPFLAGS: $CPPFLAGS"
        log "  LDFLAGS: $LDFLAGS"
        log "  LIBS: $LIBS"
        log "  PKG_CONFIG_PATH: $PKG_CONFIG_PATH"

        # Configure SASL with explicit OpenSSL paths and comprehensive options
        ./configure \
            --disable-shared \
            --enable-static \
            --prefix="$sasl_prefix" \
            --with-openssl="$openssl_prefix" \
            --with-ssl="$openssl_prefix" \
            --enable-gssapi="$krb5_prefix" \
            --without-dblib \
            --disable-gdbm \
            --disable-sample \
            --disable-obsolete_cram_attr \
            --disable-obsolete_digest_attr \
            --without-pam \
            --without-saslauthd \
            --without-pwcheck \
            --without-des \
            --without-authdaemond \
            --disable-java \
            --disable-sql \
            --disable-ldapdb \
            --enable-plain \
            --enable-login \
            --enable-digest \
            --enable-cram \
            --enable-otp \
            --enable-ntlm \
            --enable-scram \
            ac_cv_func_getnameinfo=yes \
            ac_cv_func_getaddrinfo=yes \
            OPENSSL_CFLAGS="-I$openssl_prefix/include" \
            OPENSSL_LIBS="-L$openssl_lib_dir -lssl -lcrypto"

        log "Configuration completed, starting build..."

        # Build with reduced parallelism to avoid race conditions
        make -j$(( $(get_cpu_count) / 2 )) || {
            log "Build failed, trying single-threaded build..."
            make clean
            make -j1
        }

        # Install
        make install

        # Verify the build
        if [ ! -f "$sasl_prefix/lib/libsasl2.a" ]; then
            error "Failed to build static Cyrus SASL"
        fi

        # Additional verification - check if OTP plugin was built (this was failing)
        if [ -f "$sasl_prefix/lib/sasl2/libotp.a" ] || [ -f "$sasl_prefix/lib/sasl2/libotp.la" ]; then
            log "✅ OTP plugin built successfully"
        else
            log "⚠️  OTP plugin may not have built, but core SASL library is available"
        fi

        log "✅ Static Cyrus SASL built successfully"
    else
        log "Static Cyrus SASL already built, skipping..."
    fi
}

# Build static library with musl-specific configuration
build_static_lib_musl() {
    local lib_name="$1"
    local prefix="$2"
    local source_dir="$3"
    local configure_args="$4"

    cd "$source_dir"

    local lib_file="$prefix/lib/lib${lib_name}.a"
    if [ ! -f "$lib_file" ]; then
        log "Configuring and building static $lib_name for musl..."
        make clean 2>/dev/null || true

        setup_musl_compiler

        # Run configure with provided arguments
        eval "./configure --prefix=\"$prefix\" $configure_args"

        make -j$(get_cpu_count)
        make install

        # Verify the build
        if [ ! -f "$lib_file" ]; then
            error "Failed to build static $lib_name"
        fi

        log "✅ Static $lib_name built successfully"
    else
        log "Static $lib_name already built, skipping..."
    fi
}

# Check common and musl-specific dependencies
check_common_dependencies
check_musl_dependencies

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
log "Building self-contained librdkafka.so for Linux x86_64 musl (WITH SASL)"
log "Dependencies to build:"
log "  - OpenSSL: $OPENSSL_VERSION"
log "  - Cyrus SASL: $CYRUS_SASL_VERSION (with official GCC 14+ patches)"
log "  - MIT Kerberos: $KRB5_VERSION"
log "  - zlib: $ZLIB_VERSION"
log "  - ZStd: $ZSTD_VERSION"
log "librdkafka source: $LIBRDKAFKA_TARBALL"
log "Build directory: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Build OpenSSL
log "Building OpenSSL $OPENSSL_VERSION for musl..."
OPENSSL_PREFIX="$DEPS_PREFIX/static-openssl-$OPENSSL_VERSION"
OPENSSL_TARBALL="openssl-$OPENSSL_VERSION.tar.gz"
OPENSSL_DIR="openssl-$OPENSSL_VERSION"

secure_download "$(get_openssl_url)" "$OPENSSL_TARBALL"
extract_if_needed "$OPENSSL_TARBALL" "$OPENSSL_DIR"
build_openssl_musl "$OPENSSL_PREFIX" "$OPENSSL_DIR"

cd "$BUILD_DIR"

# Determine OpenSSL lib directory (could be lib or lib64)
if [ -f "$OPENSSL_PREFIX/lib64/libssl.a" ]; then
    OPENSSL_LIB_DIR="$OPENSSL_PREFIX/lib64"
    log "Using OpenSSL libraries from lib64"
else
    OPENSSL_LIB_DIR="$OPENSSL_PREFIX/lib"
    log "Using OpenSSL libraries from lib"
fi

# Build MIT Kerberos
log "Building MIT Kerberos $KRB5_VERSION for musl..."
KRB5_PREFIX="$DEPS_PREFIX/static-krb5-$KRB5_VERSION"
KRB5_TARBALL="krb5-$KRB5_VERSION.tar.gz"
KRB5_DIR="krb5-$KRB5_VERSION"

secure_download "$(get_krb5_url)" "$KRB5_TARBALL"
extract_if_needed "$KRB5_TARBALL" "$KRB5_DIR"
build_krb5_musl "$KRB5_PREFIX" "$KRB5_DIR"

cd "$BUILD_DIR"

# Build SASL with official patches (updated call)
log "Building Cyrus SASL $CYRUS_SASL_VERSION for musl..."
SASL_PREFIX="$DEPS_PREFIX/static-sasl-$CYRUS_SASL_VERSION"
SASL_TARBALL="cyrus-sasl-$CYRUS_SASL_VERSION.tar.gz"
SASL_DIR="cyrus-sasl-$CYRUS_SASL_VERSION"

secure_download "$(get_sasl_url)" "$SASL_TARBALL"
extract_if_needed "$SASL_TARBALL" "$SASL_DIR"
build_sasl_musl "$SASL_PREFIX" "$SASL_DIR" "$KRB5_PREFIX" "$OPENSSL_PREFIX"  # Added OPENSSL_PREFIX parameter

cd "$BUILD_DIR"

# Build zlib
log "Building zlib $ZLIB_VERSION for musl..."
ZLIB_PREFIX="$DEPS_PREFIX/static-zlib-$ZLIB_VERSION"
ZLIB_TARBALL="zlib-$ZLIB_VERSION.tar.gz"
ZLIB_DIR="zlib-$ZLIB_VERSION"

secure_download "$(get_zlib_url)" "$ZLIB_TARBALL"
extract_if_needed "$ZLIB_TARBALL" "$ZLIB_DIR"
build_static_lib_musl "z" "$ZLIB_PREFIX" "$ZLIB_DIR" "--static"

cd "$BUILD_DIR"

# Build ZStd
log "Building ZStd $ZSTD_VERSION for musl..."
ZSTD_PREFIX="$DEPS_PREFIX/static-zstd-$ZSTD_VERSION"
ZSTD_TARBALL="zstd-$ZSTD_VERSION.tar.gz"
ZSTD_DIR="zstd-$ZSTD_VERSION"

secure_download "$(get_zstd_url)" "$ZSTD_TARBALL"
extract_if_needed "$ZSTD_TARBALL" "$ZSTD_DIR"
cd "$ZSTD_DIR"

if [ ! -f "$ZSTD_PREFIX/lib/libzstd.a" ]; then
    log "Building ZStd for musl..."
    make clean 2>/dev/null || true

    setup_musl_compiler

    # Build static library using ZStd's Makefile
    make lib-mt CFLAGS="$CFLAGS" PREFIX="$ZSTD_PREFIX" -j$(get_cpu_count)
    make install PREFIX="$ZSTD_PREFIX"

    # Verify the build
    if [ ! -f "$ZSTD_PREFIX/lib/libzstd.a" ]; then
        error "Failed to build static ZStd"
    fi

    log "✅ Static ZStd built successfully"
else
    log "Static ZStd already built, skipping..."
fi

cd "$BUILD_DIR"

# Extract and patch librdkafka
log "Extracting librdkafka..."
tar xzf "$LIBRDKAFKA_TARBALL"
cd "librdkafka-$LIBRDKAFKA_VERSION"

# Fix permissions and apply patches
fix_configure_permissions
apply_patches PATCHES_FOUND

# Configure librdkafka for musl (with SASL)
log "Configuring librdkafka for musl (with SASL support)..."

setup_musl_compiler

# musl-specific configuration with SASL
export CPPFLAGS="$CPPFLAGS -I$KRB5_PREFIX/include -I$SASL_PREFIX/include"
export LDFLAGS="$LDFLAGS -L$KRB5_PREFIX/lib -L$SASL_PREFIX/lib"

if [ -f configure ]; then
    log "Using mklove configure script"

    # mklove-specific configure options with SASL
    ./configure \
        --enable-static \
        --disable-shared \
        --disable-curl \
        --enable-gssapi \
        --enable-sasl \
        --disable-c11threads
else
    error "No configure script found"
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

# Create self-contained shared library for musl (with SASL)
log "Creating self-contained librdkafka.so for musl (with SASL support)..."

# First, verify all static libraries exist
log "Verifying static libraries exist..."
for lib_path in \
    "$SASL_PREFIX/lib/libsasl2.a" \
    "$KRB5_PREFIX/lib/libgssapi_krb5.a" \
    "$KRB5_PREFIX/lib/libkrb5.a" \
    "$KRB5_PREFIX/lib/libk5crypto.a" \
    "$KRB5_PREFIX/lib/libcom_err.a" \
    "$KRB5_PREFIX/lib/libkrb5support.a" \
    "$OPENSSL_LIB_DIR/libssl.a" \
    "$OPENSSL_LIB_DIR/libcrypto.a" \
    "$ZLIB_PREFIX/lib/libz.a" \
    "$ZSTD_PREFIX/lib/libzstd.a"
do
    if [ ! -f "$lib_path" ]; then
        error "Required static library not found: $lib_path"
    else
        log "✅ Found: $lib_path"
    fi
done

echo '
{
  global:
    rd_kafka_*;
  local:
    *;
};
' > export.map

gcc -shared -fPIC \
  -Wl,--version-script=export.map \
  -Wl,--whole-archive src/librdkafka.a -Wl,--no-whole-archive \
  -o librdkafka.so \
  -Wl,-Bstatic \
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
  -Wl,-Bdynamic \
  -lpthread -lm -ldl -lc \
  -static-libgcc \
  -Wl,--as-needed \
  -Wl,--no-undefined

if [ ! -f librdkafka.so ]; then
    error "Failed to create librdkafka.so"
fi

log "librdkafka.so created successfully"

# Enhanced verification
log "Verifying static linking of SASL..."
if command -v ldd &> /dev/null; then
    SASL_DEPS=$(ldd librdkafka.so 2>/dev/null | grep -i sasl || true)
    if [ -n "$SASL_DEPS" ]; then
        error "SASL is still dynamically linked: $SASL_DEPS"
    else
        log "✅ SASL successfully statically linked - no dynamic SASL dependencies found"
    fi

    # Check for any other problematic dynamic dependencies
    PROBLEMATIC_DEPS=$(ldd librdkafka.so 2>/dev/null | grep -E "(libssl|libcrypto|libz|libzstd|libkrb|libgssapi)" || true)
    if [ -n "$PROBLEMATIC_DEPS" ]; then
        warn "Found other dynamic dependencies that should be static: $PROBLEMATIC_DEPS"
    else
        log "✅ All major dependencies appear to be statically linked"
    fi
else
    log "ldd not available, skipping dynamic dependency check"
fi

# Additional verification using nm if available
if command -v nm &> /dev/null; then
    log "Checking for SASL symbols in the library..."
    SASL_SYMBOLS=$(nm -D librdkafka.so 2>/dev/null | grep -i sasl | head -5 || true)
    if [ -n "$SASL_SYMBOLS" ]; then
        log "✅ SASL symbols found in library (first 5):"
        echo "$SASL_SYMBOLS"
    else
        warn "No SASL symbols found - this might indicate a linking issue"
    fi
fi

# Verify the build (musl-compatible)
log "Verifying musl build..."
if command -v file &> /dev/null; then
    file librdkafka.so
else
    log "file command not available, skipping file type check"
fi

log "Checking dependencies with ldd:"
if command -v ldd &> /dev/null; then
    ldd librdkafka.so
else
    log "ldd not available, skipping dependency check"
fi

log "Checking for external dependencies (should only show musl and system libraries):"
if command -v ldd &> /dev/null; then
    EXTERNAL_DEPS=$(ldd librdkafka.so 2>/dev/null | grep -v "musl" | grep -v "ld-musl" | grep -v "librdkafka.so" | grep -v "=>" | grep -v "statically linked" | grep -v "not a dynamic executable" || true)
    if [ -n "$EXTERNAL_DEPS" ]; then
        # Filter out expected system dependencies that are OK on musl
        FILTERED_DEPS=$(echo "$EXTERNAL_DEPS" | grep -v "linux-vdso.so" | grep -v "/lib/ld-musl" || true)
        if [ -n "$FILTERED_DEPS" ]; then
            warn "Found some external dependencies (may be acceptable for musl): $FILTERED_DEPS"
        else
            log "✅ All dependencies are musl system libraries - build is self-contained!"
        fi
    else
        log "✅ No unexpected external dependencies found - library is self-contained!"
    fi
else
    log "ldd not available, skipping external dependency check"
fi

# Check for musl compatibility
log "Verifying musl compatibility..."
if command -v ldd &> /dev/null; then
    if ldd librdkafka.so 2>&1 | grep -q "musl"; then
        log "✅ Library is linked against musl libc"
    elif ldd librdkafka.so 2>&1 | grep -q "statically linked"; then
        log "✅ Library appears to be statically linked (good for musl)"
    else
        warn "Library linking may not be optimal for musl systems"
    fi
else
    log "ldd not available, skipping musl compatibility check"
fi

# Copy to output directory
OUTPUT_DIR="$SCRIPT_DIR"
cp librdkafka.so "$OUTPUT_DIR/"
log "librdkafka.so copied to: $OUTPUT_DIR/librdkafka.so"

# Print summaries
print_security_summary

# Enhanced summary for musl (with SASL)
log "Build completed successfully!"
log "📦 Self-contained librdkafka built for Linux x86_64 musl:"
log "   ✅ OpenSSL $OPENSSL_VERSION (SSL/TLS support) - checksum verified"
log "   ✅ Cyrus SASL $CYRUS_SASL_VERSION (authentication for AWS MSK) - checksum verified"
log "   ✅ MIT Kerberos $KRB5_VERSION (GSSAPI/Kerberos authentication) - checksum verified"
log "   ✅ zlib $ZLIB_VERSION (compression) - checksum verified"
log "   ✅ ZStd $ZSTD_VERSION (high-performance compression) - checksum verified"
log ""
log "🎯 Ready for deployment on musl-based systems (Alpine Linux, Docker containers)"
log "☁️  Compatible with AWS MSK and other secured Kafka clusters"
log "🔐 Supply chain security: All dependencies cryptographically verified"
log "🐳 Docker-optimized: Perfect for Alpine-based container deployments"
log "🛠️  GCC 14+ compatible: Official patches applied to Cyrus SASL"
log ""
log "Location: $OUTPUT_DIR/librdkafka.so"

# Cleanup
cleanup_build_dir "$BUILD_DIR"

# Reset environment variables
unset CFLAGS CXXFLAGS CPPFLAGS LDFLAGS CC

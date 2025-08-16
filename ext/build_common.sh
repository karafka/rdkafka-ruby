#!/usr/bin/env bash
#
# Common functions and constants for librdkafka builds
# This file should be sourced by platform-specific build scripts
#
# Usage: source "$(dirname "${BASH_SOURCE[0]}")/build_common.sh"
#

# Prevent multiple sourcing
if [[ "${BUILD_COMMON_SOURCED:-}" == "1" ]]; then
    return 0
fi

BUILD_COMMON_SOURCED=1

# Version constants - update these to upgrade dependencies
readonly OPENSSL_VERSION="3.0.16"
readonly CYRUS_SASL_VERSION="2.1.28"
readonly ZLIB_VERSION="1.3.1"
readonly ZSTD_VERSION="1.5.7"
readonly KRB5_VERSION="1.21.3"
readonly LIBRDKAFKA_VERSION="2.8.0"

# SHA256 checksums for supply chain security
# Update these when upgrading versions
declare -A CHECKSUMS=(
    ["openssl-${OPENSSL_VERSION}.tar.gz"]="57e03c50feab5d31b152af2b764f10379aecd8ee92f16c985983ce4a99f7ef86"
    ["cyrus-sasl-${CYRUS_SASL_VERSION}.tar.gz"]="7ccfc6abd01ed67c1a0924b353e526f1b766b21f42d4562ee635a8ebfc5bb38c"
    ["zlib-1.3.1.tar.gz"]="9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23"
    ["zstd-${ZSTD_VERSION}.tar.gz"]="eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3"
    ["krb5-${KRB5_VERSION}.tar.gz"]="b7a4cd5ead67fb08b980b21abd150ff7217e85ea320c9ed0c6dadd304840ad35"
    ["librdkafka-${LIBRDKAFKA_VERSION}.tar.gz"]="5bd1c46f63265f31c6bfcedcde78703f77d28238eadf23821c2b43fc30be3e25"s
)

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

security_log() {
    echo -e "${BLUE}[SECURITY] $1${NC}"
}

# Function to verify checksums
verify_checksum() {
    local file="$1"
    local expected_checksum="${CHECKSUMS[$file]}"

    if [ -z "$expected_checksum" ]; then
        error "No checksum defined for $file - this is a security risk!"
    fi

    security_log "Verifying checksum for $file..."
    local actual_checksum

    # Use platform-appropriate checksum command
    if command -v sha256sum &> /dev/null; then
        actual_checksum=$(sha256sum "$file" | cut -d' ' -f1)
    elif command -v shasum &> /dev/null; then
        actual_checksum=$(shasum -a 256 "$file" | cut -d' ' -f1)
    else
        error "No SHA256 checksum utility found (tried sha256sum, shasum)"
    fi

    if [ "$actual_checksum" = "$expected_checksum" ]; then
        security_log "✅ Checksum verified for $file"
        return 0
    else
        error "❌ CHECKSUM MISMATCH for $file!
Expected: $expected_checksum
Actual:   $actual_checksum
This could indicate a supply chain attack or corrupted download!"
    fi
}

# Function to securely download and verify files
secure_download() {
    local url="$1"
    local filename="$2"

    # Check if file already exists in current directory (may have been already downloaded)
    if [ -f "$filename" ]; then
        log "File $filename already exists, verifying checksum..."
        verify_checksum "$filename"
        return 0
    fi

    # Check dist directory relative to script location
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local dist_file="$script_dir/../dist/$filename"

    if [ -f "$dist_file" ]; then
        log "Using distributed $filename from dist/"
        cp "$dist_file" "$filename"
        verify_checksum "$filename"
        return 0
    fi

    log "Downloading $filename from $url..."

    # Use platform-appropriate download command
    if command -v wget &> /dev/null; then
        # Linux - use wget with security options
        if ! wget --secure-protocol=TLSv1_2 \
                 --https-only \
                 --timeout=30 \
                 --tries=3 \
                 --progress=bar \
                 "$url" \
                 -O "$filename"; then
            error "Failed to download $filename from $url"
        fi
    elif command -v curl &> /dev/null; then
        # macOS/fallback - use curl with security options
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

# Function to detect CPU count for parallel builds
get_cpu_count() {
    if command -v nproc &> /dev/null; then
        nproc
    elif command -v sysctl &> /dev/null; then
        sysctl -n hw.ncpu
    else
        echo "4"  # fallback
    fi
}

# Function to auto-detect librdkafka tarball
find_librdkafka_tarball() {
    local dist_dir="$1"
    local tarball="$dist_dir/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz"

    if [ ! -f "$tarball" ]; then
        error "librdkafka-${LIBRDKAFKA_VERSION}.tar.gz not found in $dist_dir"
    fi

    echo "$tarball"
}

# Function to find and validate patches
find_patches() {
    local patches_dir="$1"
    local -n patches_array=$2  # nameref to output array

    patches_array=()

    if [ -d "$patches_dir" ]; then
        while IFS= read -r -d '' patch; do
            patches_array+=("$patch")
        done < <(find "$patches_dir" -name "*.patch" -type f -print0 | sort -z)

        if [ ${#patches_array[@]} -gt 0 ]; then
            log "Found ${#patches_array[@]} patches to apply:"
            for patch in "${patches_array[@]}"; do
                log "  - $(basename "$patch")"
            done
        else
            log "No patches found in $patches_dir"
        fi
    else
        log "No patches directory found: $patches_dir"
    fi
}

# Function to apply patches
apply_patches() {
    local -n patches_array=$1  # nameref to patches array

    if [ ${#patches_array[@]} -gt 0 ]; then
        log "Applying Ruby-specific patches..."
        for patch in "${patches_array[@]}"; do
            log "Applying patch: $(basename "$patch")"
            if patch -p1 < "$patch"; then
                log "✅ Successfully applied $(basename "$patch")"
            else
                error "❌ Failed to apply patch: $(basename "$patch")"
            fi
        done
        log "All patches applied successfully"
    fi
}

# Function to verify librdkafka tarball checksum if available
verify_librdkafka_checksum() {
    local tarball="$1"
    local filename
    filename=$(basename "$tarball")

    if [ -n "${CHECKSUMS[$filename]:-}" ]; then
        local current_dir
        current_dir=$(pwd)
        cd "$(dirname "$tarball")"
        verify_checksum "$filename"
        cd "$current_dir"
    else
        warn "No checksum defined for $filename - consider adding one for security"
    fi
}

# Function to set execute permissions on configure scripts
fix_configure_permissions() {
    log "Setting execute permissions on configure scripts..."
    chmod +x configure* 2>/dev/null || true
    chmod +x mklove/modules/configure.* 2>/dev/null || true
}

# Function to print security summary
print_security_summary() {
    security_log "🔒 SECURITY VERIFICATION COMPLETE"
    security_log "All dependencies downloaded and verified with SHA256 checksums"
    security_log "Supply chain integrity maintained throughout build process"
}

# Function to print build summary
print_build_summary() {
    local platform="$1"
    local arch="$2"
    local output_dir="$3"
    local library_name="$4"

    log "Build completed successfully!"
    log "📦 Self-contained librdkafka built for $platform $arch:"
    log "   ✅ OpenSSL $OPENSSL_VERSION (SSL/TLS support) - checksum verified"
    log "   ✅ Cyrus SASL $CYRUS_SASL_VERSION (authentication for AWS MSK) - checksum verified"
    log "   ✅ MIT Kerberos $KRB5_VERSION (GSSAPI/Kerberos authentication) - checksum verified"
    log "   ✅ zlib $ZLIB_VERSION (compression) - checksum verified"
    log "   ✅ ZStd $ZSTD_VERSION (high-performance compression) - checksum verified"
    log ""
    log "🎯 Ready for deployment on $platform systems"
    log "☁️  Compatible with AWS MSK and other secured Kafka clusters"
    log "🔐 Supply chain security: All dependencies cryptographically verified"
    log ""
    log "Location: $output_dir/$library_name"
}

# Function to clean up build directory with user prompt (except .tar.gz files in CI)
cleanup_build_dir() {
    local build_dir="$1"

    if [ "${CI:-}" = "true" ]; then
        # In CI: remove everything except .tar.gz files without prompting
        echo "CI detected: cleaning up $build_dir (preserving .tar.gz files for caching)"

        # First, find and move all .tar.gz files to a temp location
        temp_dir=$(mktemp -d)
        find "$build_dir" -name "*.tar.gz" -exec mv {} "$temp_dir/" \; 2>/dev/null || true

        # Remove everything in build_dir
        rm -rf "$build_dir"/* 2>/dev/null || true
        rm -rf "$build_dir"/.* 2>/dev/null || true

        # Move .tar.gz files back
        mv "$temp_dir"/* "$build_dir/" 2>/dev/null || true
        rmdir "$temp_dir" 2>/dev/null || true

        log "Build directory cleaned up (preserved .tar.gz files)"
    else
        # Interactive mode: prompt user
        echo
        read -p "Remove build directory $build_dir? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$build_dir"
            log "Build directory cleaned up"
        fi
    fi
}

# Function to validate build environment
check_common_dependencies() {
    log "Checking common build dependencies..."

    local missing_tools=()

    command -v tar &> /dev/null || missing_tools+=("tar")
    command -v make &> /dev/null || missing_tools+=("make")
    command -v patch &> /dev/null || missing_tools+=("patch")

    # Check for download tools
    if ! command -v wget &> /dev/null && ! command -v curl &> /dev/null; then
        missing_tools+=("wget or curl")
    fi

    # Check for checksum tools
    if ! command -v sha256sum &> /dev/null && ! command -v shasum &> /dev/null; then
        missing_tools+=("sha256sum or shasum")
    fi

    if [ ${#missing_tools[@]} -gt 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
    fi

    log "✅ Common build tools found"
}

# Function to extract tarball if directory doesn't exist
extract_if_needed() {
    local tarball="$1"
    local expected_dir="$2"

    if [ ! -d "$expected_dir" ]; then
        log "Extracting $(basename "$tarball")..."
        tar xzf "$tarball"
    else
        log "Directory $expected_dir already exists, skipping extraction"
    fi
}

# Download URLs for dependencies
get_openssl_url() {
    echo "https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz"
}

get_sasl_url() {
    echo "https://github.com/cyrusimap/cyrus-sasl/releases/download/cyrus-sasl-${CYRUS_SASL_VERSION}/cyrus-sasl-${CYRUS_SASL_VERSION}.tar.gz"
}

get_zlib_url() {
    echo "https://github.com/madler/zlib/releases/download/v${ZLIB_VERSION}/zlib-${ZLIB_VERSION}.tar.gz"
}

get_zstd_url() {
    echo "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
}

get_krb5_url() {
    # Using MIT mirror since kerberos.org is down
    # echo "https://kerberos.org/dist/krb5/${KRB5_VERSION%.*}/krb5-${KRB5_VERSION}.tar.gz"
    echo "https://web.mit.edu/kerberos/dist/krb5/${KRB5_VERSION%.*}/krb5-${KRB5_VERSION}.tar.gz"
}

# Export functions and variables that scripts will need
export -f log warn error security_log
export -f verify_checksum secure_download get_cpu_count
export -f find_librdkafka_tarball find_patches apply_patches
export -f verify_librdkafka_checksum fix_configure_permissions
export -f print_security_summary print_build_summary cleanup_build_dir
export -f check_common_dependencies extract_if_needed
export -f get_openssl_url get_sasl_url get_zlib_url get_zstd_url get_krb5_url

# Export constants
export OPENSSL_VERSION CYRUS_SASL_VERSION ZLIB_VERSION ZSTD_VERSION KRB5_VERSION
export RED GREEN YELLOW BLUE NC

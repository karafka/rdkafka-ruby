#!/usr/bin/env bash
#
# Local test script for ARM Alpine (aarch64-linux-musl) build
# This script builds librdkafka for ARM64 Alpine using QEMU emulation
#
# Requirements:
#   - Docker installed and running
#   - QEMU support (this script will set it up if needed)
#   - ~45-60 minutes to complete
#   - ~10GB free disk space
#
# Usage:
#   ./doc.sh
#
set -e

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║    ARM Alpine (aarch64-linux-musl) Build Test                           ║"
echo "║    This will take 30-45 minutes due to QEMU emulation                   ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed or not in PATH"
    echo "   Please install Docker from https://docker.com"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker daemon is not running"
    echo "   Please start Docker and try again"
    exit 1
fi

echo "✅ Docker is available and running"
echo ""

# Check current directory
if [ ! -f "ext/build_linux_aarch64_musl.sh" ]; then
    echo "❌ Error: Must be run from rdkafka-ruby root directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected file: ext/build_linux_aarch64_musl.sh"
    exit 1
fi

echo "✅ Running from correct directory"
echo ""

# Set up QEMU for multi-arch support
echo "🔧 Setting up QEMU for ARM64 emulation..."
echo ""

# Try to set up QEMU using the official image
if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes &> /dev/null; then
    echo "✅ QEMU ARM64 emulation enabled successfully"
elif docker run --rm --privileged tonistiigi/binfmt --install arm64 &> /dev/null; then
    echo "✅ QEMU ARM64 emulation enabled successfully (using binfmt)"
else
    echo "⚠️  Warning: Could not automatically set up QEMU"
    echo ""
    echo "Checking if QEMU is already configured..."

    # Test if ARM64 works
    if docker run --rm --platform linux/arm64 alpine:3.23 uname -m &> /dev/null; then
        echo "✅ QEMU already working! Continuing..."
    else
        echo ""
        echo "❌ QEMU setup failed. Manual setup required:"
        echo ""
        echo "Option 1 - Use Docker Desktop (easiest):"
        echo "  1. Install Docker Desktop (includes QEMU)"
        echo "  2. Enable 'Use containerd for pulling and storing images' in settings"
        echo "  3. Restart Docker Desktop"
        echo ""
        echo "Option 2 - Manual QEMU setup (Linux):"
        echo "  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes"
        echo ""
        echo "Option 3 - Install QEMU packages (Linux):"
        echo "  # On Ubuntu/Debian:"
        echo "  sudo apt-get install qemu-user-static binfmt-support"
        echo "  # On macOS (use Docker Desktop instead)"
        echo "  # On Arch Linux:"
        echo "  sudo pacman -S qemu-user-static qemu-user-static-binfmt"
        echo ""
        exit 1
    fi
fi

echo ""

# Clean up any existing build artifacts
if [ -f "ext/librdkafka.so" ]; then
    echo "🧹 Cleaning up existing librdkafka.so"
    rm -f ext/librdkafka.so
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Starting build..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Phase 1: Verifying ARM64 support..."
echo ""

# Test ARM64 works
ARCH_TEST=$(docker run --rm --platform linux/arm64 alpine:3.23 uname -m)
if [ "$ARCH_TEST" = "aarch64" ]; then
    echo "✅ ARM64 emulation working (architecture: $ARCH_TEST)"
else
    echo "❌ ARM64 emulation not working properly (got: $ARCH_TEST)"
    exit 1
fi

echo ""
echo "Phase 2: Pulling Alpine ARM64 image (if needed)..."
echo ""

# Start timer
START_TIME=$(date +%s)

# Run the build
docker run --rm --platform linux/arm64 \
  -v "$(pwd):/workspace" \
  -w /workspace \
  alpine:3.23 \
  sh -c '
    set -e
    echo ""
    echo "Phase 3: Installing build dependencies (~5 minutes)..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    apk add --no-cache \
      git curl ca-certificates build-base linux-headers \
      pkgconf perl autoconf automake libtool bison flex file bash wget \
      zstd-dev openssl-dev cyrus-sasl-dev cyrus-sasl cyrus-sasl-login \
      cyrus-sasl-crammd5 cyrus-sasl-digestmd5 cyrus-sasl-gssapiv2 cyrus-sasl-scram \
      krb5-libs openssl zlib zlib-dev zstd-libs

    echo ""
    echo "Phase 4: Building librdkafka for ARM64 Alpine (~35-40 minutes)..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "This will build:"
    echo "  • OpenSSL 3.0.16"
    echo "  • MIT Kerberos 1.21.3"
    echo "  • Cyrus SASL 2.1.28"
    echo "  • zlib 1.3.1"
    echo "  • ZStd 1.5.7"
    echo "  • librdkafka (latest)"
    echo ""
    echo "Please be patient, QEMU emulation is slower than native..."
    echo ""

    cd /workspace/ext
    bash ./build_linux_aarch64_musl.sh
  '

# Calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Build completed successfully! 🎉"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "⏱️  Total time: ${MINUTES}m ${SECONDS}s"
echo ""

# Verify the build
if [ -f "ext/librdkafka.so" ]; then
    echo "✅ librdkafka.so created successfully"
    echo ""
    echo "📊 File information:"
    ls -lh ext/librdkafka.so
    echo ""
    echo "🔍 File type:"
    file ext/librdkafka.so
    echo ""
    echo "📦 Library details:"
    docker run --rm --platform linux/arm64 -v "$(pwd):/workspace" alpine:3.23 sh -c '
        apk add --no-cache file binutils > /dev/null 2>&1
        echo "Architecture: $(file /workspace/ext/librdkafka.so | grep -o "ARM aarch64" || echo "Unknown")"
        echo "Size: $(du -h /workspace/ext/librdkafka.so | cut -f1)"
        echo ""
        echo "Checking dependencies (should only show musl and system libs):"
        ldd /workspace/ext/librdkafka.so 2>/dev/null || echo "Not a dynamic executable (good!)"
    '
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✨ Success! The ARM Alpine binary is ready for deployment."
    echo ""
    echo "Next steps:"
    echo "  1. Commit the changes to Git"
    echo "  2. Create a PR to test in GitHub Actions"
    echo "  3. On release, this binary will be published to RubyGems"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
    echo "❌ Error: librdkafka.so was not created"
    echo "   Check the build output above for errors"
    exit 1
fi

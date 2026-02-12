#!/usr/bin/env bash
#
# Simplified local test script - disables OpenSSL assembly for QEMU stability
#
set -e

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║    ARM Alpine Build Test (QEMU-Stable Mode)                             ║"
echo "║    Assembly optimizations disabled for local testing                    ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check Docker
if ! command -v docker &> /dev/null || ! docker info &> /dev/null; then
    echo "❌ Docker not available"
    exit 1
fi

# Check directory
if [ ! -f "ext/build_linux_aarch64_musl.sh" ]; then
    echo "❌ Must run from rdkafka-ruby root"
    exit 1
fi

echo "✅ Prerequisites OK"
echo ""

# Set up QEMU
echo "🔧 Setting up QEMU..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes &> /dev/null || \
docker run --rm --privileged tonistiigi/binfmt --install arm64 &> /dev/null || \
echo "QEMU already set up"

# Test ARM64
ARCH_TEST=$(docker run --rm --platform linux/arm64 alpine:3.23 uname -m 2>/dev/null)
if [ "$ARCH_TEST" != "aarch64" ]; then
    echo "❌ ARM64 not working"
    exit 1
fi

echo "✅ ARM64 emulation ready"
echo ""

# Clean up
rm -rf ext/build-tmp-musl-aarch64

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Starting build (OpenSSL: no-asm mode for QEMU stability)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

START_TIME=$(date +%s)

docker run --rm --platform linux/arm64 \
  -v "$(pwd):/workspace" \
  -w /workspace \
  -e "OPENSSL_NO_ASM=1" \
  alpine:3.23 \
  sh -c '
    set -e
    echo "Installing dependencies..."
    apk add --no-cache \
      git curl ca-certificates build-base linux-headers \
      pkgconf perl autoconf automake libtool bison flex file bash wget \
      zstd-dev openssl-dev cyrus-sasl-dev cyrus-sasl cyrus-sasl-login \
      cyrus-sasl-crammd5 cyrus-sasl-digestmd5 cyrus-sasl-gssapiv2 cyrus-sasl-scram \
      krb5-libs openssl zlib zlib-dev zstd-libs > /dev/null

    echo ""
    echo "Building librdkafka..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd /workspace/ext
    bash ./build_linux_aarch64_musl.sh
  '

END_TIME=$(date +%s)
MINUTES=$(( (END_TIME - START_TIME) / 60 ))
SECONDS=$(( (END_TIME - START_TIME) % 60 ))

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Build completed! 🎉"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "⏱️  Total time: ${MINUTES}m ${SECONDS}s"
echo ""

if [ -f "ext/librdkafka.so" ]; then
    echo "✅ librdkafka.so created successfully"
    echo ""
    ls -lh ext/librdkafka.so
    file ext/librdkafka.so
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✨ Success! Build logic validated."
    echo ""
    echo "⚠️  NOTE: This used OpenSSL no-asm mode for QEMU stability."
    echo "   GitHub Actions will use full optimizations (normal -j2/-j4 builds)."
    echo ""
    echo "Ready to commit and test in GitHub Actions! 🚀"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
    echo "❌ Build failed"
    exit 1
fi

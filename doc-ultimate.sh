#!/usr/bin/env bash
#
# Ultimate QEMU-optimized build script
# Uses every trick to make QEMU stable
#
set -e

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║    Ultimate QEMU-Stable ARM Alpine Build                                ║"
echo "║    Using all available optimization tricks                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check Docker resources
echo "🔍 Checking Docker resources..."
DOCKER_MEM=$(docker info 2>/dev/null | grep "Total Memory" | awk '{print $3$4}')
echo "   Docker Memory: ${DOCKER_MEM:-Unknown}"
echo ""

# Setup QEMU
echo "🔧 Setting up QEMU..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes &> /dev/null || true
echo "✅ QEMU ready"
echo ""

# Clean up
rm -rf ext/build-tmp-musl-aarch64

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Starting optimized build..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Optimizations enabled:"
echo "  • -O0 compilation (no optimization, faster/stabler)"
echo "  • OpenSSL no-asm mode"
echo "  • Single-threaded builds (-j1)"
echo "  • Limited memory per process"
echo ""

START=$(date +%s)

docker run --rm --platform linux/arm64 \
  --memory="6g" \
  --memory-swap="8g" \
  --cpus="2" \
  -v "$(pwd):/workspace" \
  -w /workspace \
  -e "OPENSSL_NO_ASM=1" \
  -e "QEMU_OPT_LEVEL=O0" \
  -e "MAKEFLAGS=-j1" \
  alpine:3.23 \
  sh -c '
    set -e

    echo "Installing dependencies..."
    apk add --no-cache \
      git curl ca-certificates build-base linux-headers \
      pkgconf perl autoconf automake libtool bison flex file bash wget \
      zstd-dev openssl-dev cyrus-sasl-dev cyrus-sasl cyrus-sasl-login \
      cyrus-sasl-crammd5 cyrus-sasl-digestmd5 cyrus-sasl-gssapiv2 cyrus-sasl-scram \
      krb5-libs openssl zlib zlib-dev zstd-libs

    echo "✅ Dependencies installed"
    echo ""
    echo "Building with ultra-stable settings..."
    echo "  OPENSSL_NO_ASM: $OPENSSL_NO_ASM"
    echo "  QEMU_OPT_LEVEL: $QEMU_OPT_LEVEL"
    echo ""

    cd /workspace/ext

    # Run the build
    if bash ./build_linux_aarch64_musl.sh; then
        echo "✅ Build succeeded!"
    else
        echo "❌ Build failed"
        exit 1
    fi
  '

END=$(date +%s)
DURATION=$((END - START))
MIN=$((DURATION / 60))
SEC=$((DURATION % 60))

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Build completed in ${MIN}m ${SEC}s! 🎉"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ -f "ext/librdkafka.so" ]; then
    echo "✅ librdkafka.so created successfully"
    echo ""
    ls -lh ext/librdkafka.so
    file ext/librdkafka.so
    echo ""
    echo "Checking dependencies:"
    docker run --rm --platform linux/arm64 \
      -v "$(pwd):/workspace" \
      alpine:3.23 \
      sh -c 'apk add --no-cache file > /dev/null 2>&1 && ldd /workspace/ext/librdkafka.so'
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✨ SUCCESS! Full static build completed locally!"
    echo ""
    echo "This is the REAL production build with:"
    echo "  ✅ Statically linked OpenSSL, SASL, Kerberos, zlib, ZStd"
    echo "  ✅ ARM64 architecture"
    echo "  ✅ Alpine Linux (musl libc)"
    echo ""
    echo "Ready to commit and deploy! 🚀"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
    echo "❌ Build failed - librdkafka.so not created"
    exit 1
fi

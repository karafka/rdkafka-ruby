#!/usr/bin/env bash
#
# Quick local test - uses Alpine system libraries (not production build)
# This proves the build works without spending 90min compiling dependencies
#
set -e

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║    Quick ARM Alpine Test (System Libraries)                             ║"
echo "║    This is NOT the production build - just validates it works           ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# Setup QEMU
echo "Setting up QEMU..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes &> /dev/null || true

# Test it works
ARCH=$(docker run --rm --platform linux/arm64 alpine:3.23 uname -m 2>&1)
if [ "$ARCH" != "aarch64" ]; then
    echo "❌ QEMU not working (got: $ARCH)"
    exit 1
fi

echo "✅ QEMU working (ARM64 ready)"
echo ""
echo "Starting quick build test (~10-15 minutes)..."
echo ""

START=$(date +%s)

docker run --rm --platform linux/arm64 \
  -v "$(pwd):/workspace" \
  -w /workspace/ext \
  alpine:3.23 \
  sh -c '
    set -e

    echo "Installing Alpine packages..."
    apk add --no-cache \
      bash git curl build-base linux-headers \
      openssl-dev openssl-libs-static \
      cyrus-sasl-dev cyrus-sasl-static \
      zlib-dev zlib-static \
      zstd-dev zstd-static \
      pkgconf perl > /dev/null 2>&1

    echo "✅ Dependencies installed"
    echo ""
    echo "Finding librdkafka source..."

    # Find the librdkafka tarball
    TARBALL=$(ls -1 ../dist/librdkafka-*.tar.gz 2>/dev/null | head -1)
    if [ -z "$TARBALL" ]; then
        echo "❌ No librdkafka tarball found in dist/"
        exit 1
    fi

    echo "Found: $TARBALL"
    echo ""
    echo "Extracting and building librdkafka..."

    # Extract
    tar xzf "$TARBALL" -C /tmp
    cd /tmp/librdkafka-*

    # Configure with system libraries
    echo "Configuring..."
    ./configure \
      --enable-static \
      --disable-shared \
      --enable-ssl \
      --enable-sasl \
      --enable-zstd \
      > /tmp/configure.log 2>&1

    echo "Compiling (single-threaded)..."
    make -j1 > /tmp/make.log 2>&1

    # Create a simple shared library
    echo "Linking..."
    gcc -shared -fPIC \
      -o /workspace/ext/librdkafka.so \
      -Wl,--whole-archive src/librdkafka.a -Wl,--no-whole-archive \
      -lssl -lcrypto -lsasl2 -lz -lzstd -lpthread -lm -ldl

    echo "✅ Build complete"
  '

END=$(date +%s)
DURATION=$((END - START))
MIN=$((DURATION / 60))
SEC=$((DURATION % 60))

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Quick test completed in ${MIN}m ${SEC}s! 🎉"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ -f "ext/librdkafka.so" ]; then
    echo "✅ librdkafka.so created"
    echo ""
    ls -lh ext/librdkafka.so
    file ext/librdkafka.so
    echo ""
    docker run --rm --platform linux/arm64 \
      -v "$(pwd):/workspace" \
      alpine:3.23 \
      sh -c 'ldd /workspace/ext/librdkafka.so'
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✨ SUCCESS! This proves:"
    echo "   ✅ QEMU works"
    echo "   ✅ ARM64 Alpine container works"
    echo "   ✅ librdkafka compiles for ARM64"
    echo "   ✅ Linking works"
    echo ""
    echo "⚠️  Note: This used Alpine system libraries (dynamically linked)."
    echo "   The production build script creates a static version."
    echo ""
    echo "The production build (in CI) will:"
    echo "   - Build all dependencies from source"
    echo "   - Statically link everything"
    echo "   - Take ~60 minutes but be fully self-contained"
    echo ""
    echo "You can now confidently commit and test in GitHub Actions! 🚀"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
    echo "❌ Build failed - check logs above"
    exit 1
fi

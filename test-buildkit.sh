#!/usr/bin/env bash
#
# Test using Docker BuildKit (better QEMU support)
#
set -e

echo "Testing with Docker BuildKit..."
echo ""

# Enable BuildKit
export DOCKER_BUILDKIT=1

# Setup QEMU
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes &> /dev/null || true

# Create a simple Dockerfile
cat > /tmp/test-arm-build.Dockerfile << 'EOF'
FROM --platform=linux/arm64 alpine:3.23

# Install dependencies
RUN apk add --no-cache \
    bash git curl ca-certificates build-base linux-headers \
    pkgconf perl autoconf automake libtool bison flex file wget \
    zstd-dev openssl-dev cyrus-sasl-dev cyrus-sasl zlib-dev \
    krb5-libs openssl zlib-dev zstd-libs

WORKDIR /workspace

# Copy the build script
COPY ext/build_linux_aarch64_musl.sh ext/
COPY ext/build_common.sh ext/
COPY dist/ dist/

# Set environment for QEMU stability
ENV MAKEFLAGS="-j1"
ENV OPENSSL_NO_ASM=1

# Run the build
RUN cd ext && bash ./build_linux_aarch64_musl.sh
EOF

# Build using BuildKit
echo "Building with BuildKit (this uses better QEMU)..."
docker buildx build \
  --platform linux/arm64 \
  --progress=plain \
  -f /tmp/test-arm-build.Dockerfile \
  -t rdkafka-arm-test \
  . 2>&1 | tee /tmp/buildkit.log

echo ""
echo "Build completed! Extracting library..."

# Extract the built library
docker create --name rdkafka-temp rdkafka-arm-test
docker cp rdkafka-temp:/workspace/ext/librdkafka.so ext/ || echo "Failed to extract"
docker rm rdkafka-temp

if [ -f "ext/librdkafka.so" ]; then
    echo "✅ Success!"
    ls -lh ext/librdkafka.so
else
    echo "❌ Failed"
fi

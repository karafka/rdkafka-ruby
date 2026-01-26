# ssl_stress_test.rb
#
# This script is designed to stress-test the OpenSSL SSL/TLS layer under high concurrency
# to help detect regressions like the one described in OpenSSL issue #28171:
# https://github.com/openssl/openssl/issues/28171
#
# Issue summary:
#   - OpenSSL 3.0.17 introduced a concurrency-related regression.
#   - Multiple threads sharing the same SSL_CTX and making parallel TLS connections
#     (often with certificate verification enabled) can cause segmentation faults
#     due to race conditions in X509 store handling.
#   - Affected users include Python (httpx), Rust (reqwest, native-tls), and C applications.
#
# Script details:
#   - Starts 100 SSL servers using self-signed, in-memory certs on sequential localhost ports.
#   - Uses `rdkafka-ruby` to spin up 100 consumer threads that continuously create and destroy
#     SSL connections to these servers for a given duration.
#   - This mimics high TLS connection churn and aims to trigger latent SSL_CTX or X509_STORE
#     threading bugs like double-frees, memory corruption, or segmentation faults.
#
# Goal:
#   - Catch regressions early by validating that heavy concurrent SSL use does not lead to crashes.
#   - Provide a minimal and repeatable reproducer when diagnosing OpenSSL-level SSL instability.
#
# In case of a failure, segfault will happen

require "rdkafka"
require "socket"
require "openssl"

$stdout.sync = true

STARTING_PORT = 19093
NUM_PORTS = 150
BATCHES = 100
PORTS = STARTING_PORT...(STARTING_PORT + NUM_PORTS)

CONFIG = {
  "bootstrap.servers": Array.new(NUM_PORTS) { |i| "127.0.0.1:#{19093 + i}" }.join(","),
  "security.protocol": "SSL",
  "enable.ssl.certificate.verification": false
}

# Generate in-memory self-signed cert
key = OpenSSL::PKey::RSA.new(2048)

name = OpenSSL::X509::Name.parse("/CN=127.0.0.1")
cert = OpenSSL::X509::Certificate.new
cert.version = 2
cert.serial = 1
cert.subject = name
cert.issuer = name
cert.public_key = key.public_key
cert.not_before = Time.now
cert.not_after = Time.now + 3600
cert.sign(key, OpenSSL::Digest.new("SHA256"))

# Start servers on multiple ports
PORTS.map do |port|
  Thread.new do
    # Prepare SSL context
    # We do not use a shared context for the server because the goal is to stress librdkafka layer
    # and not the Ruby SSL layer
    ssl_context = OpenSSL::SSL::SSLContext.new
    ssl_context.cert = cert
    ssl_context.key = key

    tcp_server = TCPServer.new("127.0.0.1", port)
    ssl_server = OpenSSL::SSL::SSLServer.new(tcp_server, ssl_context)

    loop do
      ssl_socket = ssl_server.accept
      ssl_socket.close
    rescue => e
      # Some errors are expected and irrelevant
      next if e.message.include?("unexpected eof while reading")
    end
  end
end

timeout = 30
start = Time.now

# Wait for the servers to be available
# We want to make sure that they are available so we are sure that librdkafka actually hammers
# them
loop do
  all_up = PORTS.all? do |port|
    TCPSocket.new("127.0.0.1", port).close
    true
  rescue
    false
  end

  break if all_up

  raise "Timeout waiting for SSL servers" if Time.now - start > timeout

  sleep 0.1
end

start_time = Time.now
duration = 60 * 10 # 10 minutes - it should crash faster than that if SSL vulnerable

while Time.now - start_time < duration
  css = Array.new(BATCHES) { Rdkafka::Config.new(CONFIG) }
  csss = css.map(&:consumer)
  # This print is needed. No idea why but it increases the chances of segfault

  sleep(1)
  csss.each(&:close)
end

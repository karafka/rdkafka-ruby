# frozen_string_literal: true

# This integration test is designed to stress-test the OpenSSL SSL/TLS layer under high concurrency
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

require_relative "../test_helper"
require "socket"
require "openssl"

describe "SSL Stress Test" do
  before do
    @starting_port = 19093
    @num_ports = 150
    @batches = 100
    @ports = @starting_port...(@starting_port + @num_ports)

    @config = {
      "bootstrap.servers": Array.new(@num_ports) { |i| "127.0.0.1:#{@starting_port + i}" }.join(","),
      "security.protocol": "SSL",
      "enable.ssl.certificate.verification": false
    }

    # Generate in-memory self-signed cert
    @key = OpenSSL::PKey::RSA.new(2048)

    name = OpenSSL::X509::Name.parse("/CN=127.0.0.1")
    @cert = OpenSSL::X509::Certificate.new
    @cert.version = 2
    @cert.serial = 1
    @cert.subject = name
    @cert.issuer = name
    @cert.public_key = @key.public_key
    @cert.not_before = Time.now
    @cert.not_after = Time.now + 3600
    @cert.sign(@key, OpenSSL::Digest.new("SHA256"))

    # Start servers on multiple ports
    @server_threads = @ports.map do |port|
      Thread.new do
        # Prepare SSL context
        # We do not use a shared context for the server because the goal is to stress librdkafka
        # layer and not the Ruby SSL layer
        ssl_context = OpenSSL::SSL::SSLContext.new
        ssl_context.cert = @cert
        ssl_context.key = @key

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

    # Wait for the servers to be available
    # We want to make sure that they are available so we are sure that librdkafka actually
    # hammers them
    timeout = 30
    start = Time.now

    loop do
      all_up = @ports.all? do |port|
        TCPSocket.new("127.0.0.1", port).close
        true
      rescue
        false
      end

      break if all_up

      raise "Timeout waiting for SSL servers" if Time.now - start > timeout

      sleep 0.1
    end
  end

  after do
    @server_threads&.each(&:kill)
  end

  it "does not crash under heavy concurrent SSL connection churn" do
    start_time = Time.now
    duration = 60 * 10 # 10 minutes - it should crash faster than that if SSL vulnerable

    while Time.now - start_time < duration
      consumers = Array.new(@batches) do
        Rdkafka::Config.new(@config).consumer
      end

      # This print is needed. No idea why but it increases the chances of segfault
      $stdout.print ""

      sleep(1)
      consumers.each(&:close)
    end

    # If we reach here without segfault, the test passes
    pass
  end
end

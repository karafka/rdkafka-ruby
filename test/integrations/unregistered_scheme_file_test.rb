# frozen_string_literal: true

# This integration test verifies that rdkafka properly detects and reports specific SSL
# configuration errors when attempting to connect to an SSL-enabled Kafka broker.
#
# It also ensures that we do not statically link ssl certs into incorrect tmp cert location.
#
# These errors occur when rdkafka's underlying OpenSSL library encounters issues
# with SSL certificate validation, particularly related to file scheme handling
# and missing certificate directories.

require_relative "../test_helper"
require "socket"
require "openssl"
require "stringio"

describe "Unregistered Scheme File" do
  before do
    @captured_output = StringIO.new
    @original_logger = Rdkafka::Config.logger
    Rdkafka::Config.logger = Logger.new(@captured_output)

    # Start a dummy SSL server with self-signed certificate
    @ssl_server_thread = Thread.new do
      # Create TCP server
      tcp_server = TCPServer.new("localhost", 9099)

      # Generate self-signed certificate
      key = OpenSSL::PKey::RSA.new(2048)
      cert = OpenSSL::X509::Certificate.new
      cert.version = 2
      cert.serial = 1
      cert.subject = OpenSSL::X509::Name.parse("/DC=org/DC=ruby-test/CN=localhost")
      cert.issuer = cert.subject
      cert.public_key = key.public_key
      cert.not_before = Time.now
      cert.not_after = cert.not_before + 365 * 24 * 60 * 60 # 1 year

      # Add extensions
      ef = OpenSSL::X509::ExtensionFactory.new
      ef.subject_certificate = cert
      ef.issuer_certificate = cert
      cert.add_extension(ef.create_extension("basicConstraints", "CA:TRUE", true))
      cert.add_extension(ef.create_extension("keyUsage", "keyCertSign, cRLSign", true))
      cert.add_extension(ef.create_extension("subjectKeyIdentifier", "hash", false))
      cert.add_extension(ef.create_extension("authorityKeyIdentifier", "keyid:always", false))

      cert.sign(key, OpenSSL::Digest.new("SHA256"))

      # Create SSL context
      ssl_context = OpenSSL::SSL::SSLContext.new
      ssl_context.cert = cert
      ssl_context.key = key

      # Wrap TCP server with SSL
      ssl_server = OpenSSL::SSL::SSLServer.new(tcp_server, ssl_context)

      loop do
        client = ssl_server.accept
        client.puts("Invalid Kafka broker")
        client.close
      rescue
        # Ignore SSL server errors - they're expected
      end
    rescue
      # Ignore thread-level errors
    end

    # Give the server time to start
    sleep 1

    config = Rdkafka::Config.new(
      "bootstrap.servers": "localhost:9099",
      "security.protocol": "SSL",
      "client.id": "test-client",
      "group.id": "test-group"
    )

    @consumer = config.consumer
  end

  after do
    @consumer&.close
    @ssl_server_thread&.kill
    Rdkafka::Config.logger = @original_logger
  end

  it "does not produce unregistered scheme or missing file errors in SSL logs" do
    @consumer.subscribe("test-topic")

    # Try to poll for messages - this triggers SSL errors
    start_time = Time.now
    timeout = 5

    while Time.now - start_time < timeout
      begin
        @consumer.poll(1000)
      rescue
        break
      end
    end

    # Wait for rdkafka to finish logging errors
    sleep 2

    # Check captured logs for target error patterns
    @captured_output.rewind
    log_lines = @captured_output.readlines

    log_lines.each do |line|
      refute_includes line, "routines::unregistered scheme",
        "Found 'unregistered scheme' error in SSL logs"
      refute_includes line, "system library::No such file or directory",
        "Found 'No such file or directory' error in SSL logs"
    end
  end
end

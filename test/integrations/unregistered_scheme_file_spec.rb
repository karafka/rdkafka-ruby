# frozen_string_literal: true

# This integration test verifies that rdkafka properly detects and reports specific SSL
# configuration errors when attempting to connect to an SSL-enabled Kafka broker.
#
# It also ensures that we do not statically link ssl certs into incorrect tmp cert location.
#
# These errors occur when rdkafka's underlying OpenSSL library encounters issues
# with SSL certificate validation, particularly related to file scheme handling
# and missing certificate directories.
#
# Exit codes:
# - 0: Target error messages NOT detected after 5 seconds (test fails - errors missing)
# - 1: Target error messages detected (test passes - errors are present as expected)
# - 2: Unexpected exception occurred during test execution

require "rdkafka"
require "socket"
require "openssl"
require "stringio"
require "logger"

$stdout.sync = true

captured_output = StringIO.new
Rdkafka::Config.logger = Logger.new(captured_output)

# Start a dummy SSL server with self-signed certificate
ssl_server_thread = Thread.new do
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
end

# Give the server time to start
sleep 1

# Try connecting to the dummy SSL server
config = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9099",
  "security.protocol": "SSL",
  "client.id": "test-client",
  "group.id": "test-group"
)

begin
  consumer = config.consumer

  consumer.subscribe("test-topic")

  # Try to poll for messages - this triggers SSL errors
  start_time = Time.now
  timeout = 5

  while Time.now - start_time < timeout
    begin
      consumer.poll(1000)
    rescue
      break
    end
  end

  # Wait for rdkafka to finish logging errors
  sleep 2

  # Check captured logs for target error patterns
  captured_output.rewind
  captured_output.readlines.each do |line|
    exit(1) if line.include?("routines::unregistered scheme")
    exit(1) if line.include?("system library::No such file or directory")
  end
rescue
  exit(2)
ensure
  consumer&.close if defined?(consumer) && consumer
  ssl_server_thread&.kill
end

# Exit with 0 if target errors not detected
exit(0)

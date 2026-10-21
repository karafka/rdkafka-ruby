# frozen_string_literal: true

require "socket"
require "openssl"

# SSL server that accepts connections in a background thread until it is stopped.
#
# It stops with a flag and by closing the listener, never with `Thread#kill`: on JRuby,
# jruby-openssl turns a kill that lands in a TLS handshake into `OpenSSL::SSL::SSLError`. The
# accept loop must rescue handshake errors, so it rescues the kill too: the thread survives and
# JRuby waits for it forever at exit.
class FakeSslServer
  # @param host [String] host to listen on
  # @param port [Integer] port to listen on
  # @param ssl_context [OpenSSL::SSL::SSLContext] server context with a certificate and a key
  # @yieldparam client [OpenSSL::SSL::SSLSocket] accepted connection, closed after the block
  def initialize(host, port, ssl_context, &on_client)
    @tcp_server = TCPServer.new(host, port)
    ssl_server = OpenSSL::SSL::SSLServer.new(@tcp_server, ssl_context)
    @stopped = false

    @thread = Thread.new do
      until @stopped
        begin
          client = ssl_server.accept
          on_client&.call(client)
          client.close
        rescue
          # Failed handshakes are expected. After #stop, the closed listener raises here
        end
      end
    end
  end

  # Stops accepting connections and waits for the server thread to finish
  #
  # @param timeout [Integer] seconds to wait for the thread
  def stop(timeout = 5)
    @stopped = true
    @tcp_server.close
    @thread.join(timeout)
  end
end

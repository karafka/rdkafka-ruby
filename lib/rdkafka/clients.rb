# frozen_string_literal: true

module Rdkafka
  # Tracks live public clients so they can be closed before Ruby begins forced finalization.
  # @private
  module Clients
    @pid = Process.pid
    @mutex = Mutex.new
    @clients = ObjectSpace::WeakMap.new
    @tokens = [].freeze

    # Registers a successfully constructed client so it can be closed during process shutdown.
    # @param client [Consumer, Producer, Admin] client to close during process shutdown
    # @return [Consumer, Producer, Admin] the registered client
    def self.register(client)
      reset_after_fork

      @mutex.synchronize do
        live_clients

        # The strongly-held token is the key and the client is the weak value, so registering a
        # client never keeps it alive. Enumerating weak keys can expose reclaimed slots, so we
        # look the value up by token instead.
        token = Object.new
        @clients[token] = client
        @tokens = (@tokens + [token]).freeze
      end

      client
    end

    # Closes each live client registered in this process.
    # @return [nil]
    # @raise [StandardError] the first close error, after attempting every client
    def self.close_all
      return unless @pid == Process.pid

      clients = @mutex.synchronize { live_clients }
      first_error = nil

      clients.each do |client|
        client.close unless client.closed?
      rescue => error
        first_error ||= error
      end

      raise first_error if first_error
    end

    # Returns the live clients and releases tokens whose clients have been collected.
    # @return [Array<Consumer, Producer, Admin>] live clients
    # @private
    def self.live_clients
      clients = []
      tokens = []

      @tokens.each do |token|
        client = @clients[token]
        clients << client if client
        tokens << token if client
      end

      @tokens = tokens.freeze
      clients
    end
    private_class_method :live_clients

    # Replaces inherited state before registering a client in a forked child.
    # @return [nil]
    # @private
    def self.reset_after_fork
      return if @pid == Process.pid

      @clients = ObjectSpace::WeakMap.new
      @tokens = [].freeze
      @pid = Process.pid
      @mutex = Mutex.new
    end
    private_class_method :reset_after_fork
  end
end

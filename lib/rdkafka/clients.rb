# frozen_string_literal: true

module Rdkafka
  # Tracks live public clients so they can be closed before Ruby begins forced finalization.
  # @private
  module Clients
    # Ruby rewrote `ObjectSpace::WeakMap` in 3.3. On 3.2 and earlier its `[]` can hand back a
    # foreign or already-freed object for a key, which raises `NoMethodError` or segfaults the VM
    # the moment the value is touched. Track weakly only where that is safe and fall back to a
    # strong registry - pruned as clients close - on older Rubies. The strong registry holds an
    # open, un-closed client until shutdown, which is exactly when this cleanup needs it anyway.
    WEAK_TRACKING = Gem::Version.new(RUBY_VERSION) >= Gem::Version.new("3.3")

    @pid = Process.pid
    @mutex = Mutex.new

    if WEAK_TRACKING
      @clients = ObjectSpace::WeakMap.new
      @tokens = [].freeze
    else
      @clients = {}.freeze
    end

    # Registers a successfully constructed client so it can be closed during process shutdown.
    # @param client [Consumer, Producer, Admin] client to close during process shutdown
    # @return [Consumer, Producer, Admin] the registered client
    def self.register(client)
      reset_after_fork

      @mutex.synchronize do
        if WEAK_TRACKING
          live_clients

          # The strongly-held token is the key and the client is the weak value, so registering a
          # client never keeps it alive. Enumerating weak keys can expose reclaimed slots, so we
          # look the value up by token instead.
          token = Object.new
          @clients[token] = client
          @tokens = (@tokens + [token]).freeze
        else
          # Strong references, so drop clients that have already been closed to bound growth.
          live = @clients.reject { |_id, existing| existing.closed? }
          live[client.object_id] = client
          @clients = live.freeze
        end
      end

      client
    end

    # Closes each live client registered in this process.
    # @return [nil]
    # @raise [StandardError] the first close error, after attempting every client
    def self.close_all
      return unless @pid == Process.pid

      clients = @mutex.synchronize { current_clients }
      first_error = nil

      clients.each do |client|
        client.close unless client.closed?
      rescue => error
        first_error ||= error
      end

      raise first_error if first_error
    end

    # Returns the currently registered live clients.
    # @return [Array<Consumer, Producer, Admin>] live clients
    # @private
    def self.current_clients
      WEAK_TRACKING ? live_clients : @clients.values
    end
    private_class_method :current_clients

    # Returns the live clients and releases tokens whose clients have been collected.
    # Weak-tracking Rubies only.
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

      if WEAK_TRACKING
        @clients = ObjectSpace::WeakMap.new
        @tokens = [].freeze
      else
        @clients = {}.freeze
      end

      @pid = Process.pid
      @mutex = Mutex.new
    end
    private_class_method :reset_after_fork
  end
end

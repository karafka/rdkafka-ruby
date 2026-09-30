# frozen_string_literal: true

module Rdkafka
  # A KIP-932 share group consumer of Kafka messages.
  #
  # Share groups bring queue-like semantics to Kafka: multiple members of the same group
  # consume from the same partitions cooperatively and progress is tracked per record via
  # acknowledgements (`:accept`, `:release`, `:reject`) instead of committed offsets. Partition
  # assignment is entirely broker-driven: there is no rebalance callback and no `assign` step.
  #
  # To create a share consumer set up a {Config} with `:"group.id"` and call
  # {Config#share_consumer share_consumer} on it. The acknowledgement mode is selected with the
  # `:"share.acknowledgement.mode"` config property (`implicit`, the default, acknowledges every
  # polled record as accepted on the next poll/commit; `explicit` requires every record to be
  # acknowledged via {#acknowledge} before the next poll).
  #
  # @note The share consumer is a **preview** feature of librdkafka and requires a broker with
  #   share groups enabled (Apache Kafka 4.2.0+). Its API may change before general availability
  #   and it is not recommended for production use.
  #
  # @note The share consumer handle is **not thread-safe by design** (matching KIP-932): use one
  #   share consumer per thread. librdkafka enforces this and calls from a second thread raise
  #   an `RdkafkaError` with code `conflict`. This class is deliberately not built on
  #   {NativeKafka}: there is no background polling thread (statistics, error and log callbacks
  #   are all serviced from within {#poll}) and the native handle must never be used from a
  #   polling thread, so only the {#close} interaction is synchronized here. {#close} may be
  #   called from any thread and waits for in-flight calls to finish before tearing down.
  #
  # Preview limitations inherited from librdkafka worth knowing at this layer:
  # - `max.poll.records` (default 500) is a soft bound
  # - a blocking poll can only be ended by its timeout (no wakeup API)
  # - {#close} takes no timeout and is bounded internally by `socket.timeout.ms`
  # - failed acknowledgements are not retried automatically; outcomes are reported through
  #   {#acknowledgement_commit_callback=} or the per-partition results of {#commit_sync}
  class ShareConsumer
    include Enumerable
    include Helpers::OAuth

    # Mapping of friendly acknowledge types to their librdkafka enum values
    ACKNOWLEDGE_TYPES = {
      accept: Bindings::RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT,
      release: Bindings::RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE,
      reject: Bindings::RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT
    }.freeze

    private_constant :ACKNOWLEDGE_TYPES

    # State shared with the GC finalizer. Carrying the registered acknowledgement commit FFI
    # function next to the native handle guarantees its native trampoline outlives the consumer
    # object: the finalizer-driven destroy flushes pending acknowledgements through that
    # callback, so it must not be collected in the same GC cycle as the consumer. `creator_pid`
    # rides along so the finalizer (which must not capture `self`) can tell whether it is running
    # in the process that created the handle - librdkafka is not fork-safe, so an inherited handle
    # must never be destroyed from a forked child (its backing threads do not exist there).
    State = Struct.new(:native, :ack_callback, :creator_pid)

    private_constant :State

    # The client name librdkafka reports for this consumer (e.g. "rdkafka#consumer-1"), used to
    # correlate the global callbacks (statistics, error and the OAuthBearer token refresh
    # callback, all of which carry the client name) with this instance. Captured from the native
    # handle when the consumer is created, so it is available immediately - before the first
    # {#poll} and without OAuth configured - and stays readable after the consumer is closed or
    # inherited by a forked child.
    # @return [String] the librdkafka client name
    attr_reader :name

    # @private
    # Lets the OAuthBearer token refresh callback set the name from its callback context. It
    # carries the same `rd_kafka_name` value already captured at creation, so both paths agree.
    attr_writer :name

    # @private
    # @param native [FFI::Pointer] pointer to the native rd_kafka_share_t handle
    # @param opaque [Rdkafka::Opaque, nil] opaque used for callback context. Held here so it is
    #   not garbage collected while the native client can still reference it
    #   (Config.opaques is a WeakMap).
    def initialize(native, opaque: nil)
      @opaque = opaque
      # State shared with the GC finalizer so it can destroy the native handle (and keep the
      # registered acknowledgement callback alive while doing so) without capturing `self`
      # (which would pin the consumer and prevent collection). The creating pid rides along so
      # every teardown path can skip the native destroy in a forked child (librdkafka is not
      # fork-safe).
      @state = State.new(native, nil, Process.pid)
      # Capture the librdkafka client name up front. It is needed to correlate this consumer with
      # the global statistics and error callbacks (which carry the client name) - the common
      # downstream pattern (e.g. Karafka) filters those broadcast callbacks by matching `name`.
      #
      # `rd_kafka_name` has no `rd_kafka_share_t` overload: handing it the share handle reads an
      # unrelated field and returns garbage. The share handle's first member is the `rd_kafka_t`
      # it wraps (`rkshare_rk`), which is the same handle librdkafka passes to those callbacks, so
      # the name read from it matches the statistics `name` field. Capturing it here (rather than
      # lazily like the regular consumer) keeps it available before the first poll, without OAuth,
      # and after close or in a forked child, where the native handle must not be touched.
      # librdkafka assigns the name at creation, so no broker connection is required.
      @name = Rdkafka::Bindings.rd_kafka_name(Rdkafka::Bindings::NativeShareConsumer.new(native)[:rkshare_rk])
      # Guards handle teardown against in-flight calls: increments happen under this mutex and
      # close holds it while draining and destroying (mirrors NativeKafka's approach)
      @access_mutex = Mutex.new
      # Separate mutex for decrements so a blocked close (holding @access_mutex) cannot
      # deadlock with an operation trying to finish
      @decrement_mutex = Mutex.new
      # Number of calls currently using the native handle
      @operations_in_progress = 0

      ObjectSpace.define_finalizer(self, self.class.finalizer(@state))
    end

    # Builds the GC finalizer for a share consumer. `rd_kafka_share_destroy` internally closes
    # the consumer (sending pending acknowledgements and leaving the group) when it was not
    # closed explicitly. The acknowledgement callback function rides along in the state holder
    # so it is still alive when that close flushes acknowledgements through it.
    #
    # @private
    # @param state [State] holder carrying the native handle, the ack callback function and the
    #   creating pid
    # @return [Proc] finalizer proc that must not reference the consumer instance
    def self.finalizer(state)
      proc do
        native = state.native

        # librdkafka is not fork-safe: a handle inherited by a forked child must never be
        # destroyed there (its backing threads do not exist in the child), so only the process
        # that created it runs the native teardown.
        if native && state.creator_pid == Process.pid
          state.native = nil
          error = Rdkafka::Bindings.rd_kafka_share_destroy(native)
          Rdkafka::Bindings.rd_kafka_error_destroy(error) unless error.null?
          state.ack_callback = nil
        end
      end
    end

    # Subscribes to one or more topics letting the broker assign partitions to the members of
    # the share group.
    #
    # Wildcard (regex) subscriptions are not supported by share consumers.
    #
    # @param topics [Array<String>] One or more topic names
    # @return [nil]
    # @raise [ArgumentError] When no topics are given. librdkafka would treat an empty
    #   subscribe as an unsubscribe (unlike the regular consumer, which rejects it), which
    #   silently drops the subscription - use {#unsubscribe} for that.
    # @raise [RdkafkaError] When subscribing fails
    def subscribe(*topics)
      raise ArgumentError, "at least one topic is required (an empty subscribe would unsubscribe)" if topics.empty?

      with_native(__method__) do |native|
        tpl = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(topics.length)

        begin
          topics.each do |topic|
            Rdkafka::Bindings.rd_kafka_topic_partition_list_add(tpl, topic, Rdkafka::Bindings::RD_KAFKA_PARTITION_UA)
          end

          response = Rdkafka::Bindings.rd_kafka_share_subscribe(native, tpl)
          Rdkafka::RdkafkaError.validate!(response, "Error subscribing to '#{topics.join(", ")}'")
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
        end
      end

      nil
    end

    # Unsubscribes from all subscribed topics and leaves the share group assignment.
    #
    # @return [nil]
    # @raise [RdkafkaError] When unsubscribing fails
    def unsubscribe
      with_native(__method__) do |native|
        response = Rdkafka::Bindings.rd_kafka_share_unsubscribe(native)
        Rdkafka::RdkafkaError.validate!(response)
      end

      nil
    end

    # Returns the current subscription to topics.
    #
    # @return [Rdkafka::Consumer::TopicPartitionList]
    # @raise [RdkafkaError] When getting the subscription fails
    def subscription
      with_native(__method__) do |native|
        ptr = FFI::MemoryPointer.new(:pointer)
        response = Rdkafka::Bindings.rd_kafka_share_subscription(native, ptr)
        Rdkafka::RdkafkaError.validate!(response)

        native_tpl = ptr.read_pointer

        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native_tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_tpl)
        end
      end
    end

    # Polls for a batch of messages.
    #
    # Unlike {Consumer#poll} a single call returns a whole batch (up to `max.poll.records`,
    # which is a soft bound in the preview). In implicit acknowledgement mode every record
    # delivered by the previous poll is acknowledged as accepted by this call. In explicit mode
    # every previously delivered record must have been acknowledged via {#acknowledge}, otherwise
    # this call raises.
    #
    # Record-level errors (for example an unauthorized topic) are delivered as messages with
    # {ShareConsumer::Message#error} set rather than raised, since a batch can mix valid records
    # and errors. Callers should check {ShareConsumer::Message#error?} before using the payload.
    # Error entries do not need to be acknowledged.
    #
    # @param timeout_ms [Integer] Timeout of this poll
    # @return [Array<ShareConsumer::Message, RdkafkaError>] polled messages, empty when the
    #   timeout expired without records. A message that fails to build (see below) is an
    #   `RdkafkaError` rather than a `ShareConsumer::Message`.
    # @raise [RdkafkaError] When polling fails at the batch level (for example unacknowledged
    #   records in explicit mode)
    def poll(timeout_ms = Defaults::SHARE_CONSUMER_POLL_TIMEOUT_MS)
      with_native(__method__) do |native|
        messages_ptr = FFI::MemoryPointer.new(:pointer)
        error_ptr = Rdkafka::Bindings.rd_kafka_share_poll(native, timeout_ms, messages_ptr)

        # Destroys the native error and raises when it carries an actual error
        Rdkafka::RdkafkaError.validate!(error_ptr)

        batch = messages_ptr.read_pointer

        next [] if batch.null?

        begin
          count = Rdkafka::Bindings.rd_kafka_messages_count(batch)

          Array.new(count) do |i|
            message_ptr = Rdkafka::Bindings.rd_kafka_messages_get(batch, i)
            native_message = Rdkafka::Bindings::Message.new(message_ptr)

            # Only touch the error machinery for the rare record-level error entries
            error = if native_message[:err] == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
              nil
            else
              Rdkafka::RdkafkaError.build(native_message)
            end

            begin
              Message.new(
                native_message,
                delivery_count: Rdkafka::Bindings.rd_kafka_message_delivery_count(message_ptr),
                error: error
              )
            rescue Rdkafka::RdkafkaError => e
              # A message that fails to build (e.g. a header read error) is surfaced inline as an
              # error rather than discarding the whole batch - including the messages already
              # built - and raising, which would silently lose them. Mirrors how
              # Consumer#poll_batch handles the same failure mode.
              e
            end
          end
        ensure
          # Destroys the batch together with all the messages it contains. All message content
          # has been copied into Ruby objects at this point.
          Rdkafka::Bindings.rd_kafka_messages_destroy(batch)
        end
      end
    end

    # Acknowledges a message delivered by {#poll} in explicit acknowledgement mode
    # (`share.acknowledgement.mode` set to `explicit`).
    #
    # Acknowledgements are accumulated locally and sent to the broker on the next {#poll},
    # {#commit_sync} or {#commit_async}.
    #
    # @param message [ShareConsumer::Message] the message to acknowledge
    # @param type [Symbol] `:accept` (processed successfully), `:release` (make it available for
    #   redelivery) or `:reject` (do not deliver again, archive)
    # @return [nil]
    # @raise [ArgumentError] For an unknown acknowledge type
    # @raise [RdkafkaError] When acknowledging fails (for example in implicit mode, or for a
    #   record that is not currently acquired)
    def acknowledge(message, type = :accept)
      ack_type = ACKNOWLEDGE_TYPES.fetch(type) do
        raise ArgumentError, "Unknown acknowledge type: #{type.inspect} (expected one of #{ACKNOWLEDGE_TYPES.keys.map(&:inspect).join(", ")})"
      end

      with_native(__method__) do |native|
        response = Rdkafka::Bindings.rd_kafka_share_acknowledge_offset(
          native,
          message.topic,
          message.partition,
          message.offset,
          ack_type
        )

        # This runs once per consumed message in explicit mode, so the error prefix is only
        # built on the failure path
        unless response == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          Rdkafka::RdkafkaError.validate!(
            response,
            "Error acknowledging #{message.topic}/#{message.partition}@#{message.offset}"
          )
        end
      end

      nil
    end

    # Sends all pending acknowledgements to the broker and waits for the responses.
    #
    # @param timeout_ms [Integer] Maximum time to wait for the broker replies
    # @return [Rdkafka::Consumer::TopicPartitionList, nil] per-partition results where each
    #   partition carries the acknowledgement outcome in its `err` field, or nil when there was
    #   nothing to commit
    # @raise [RdkafkaError] When the commit fails as a whole
    def commit_sync(timeout_ms = Defaults::SHARE_CONSUMER_COMMIT_SYNC_TIMEOUT_MS)
      with_native(__method__) do |native|
        tpl_ptr = FFI::MemoryPointer.new(:pointer)
        error_ptr = Rdkafka::Bindings.rd_kafka_share_commit_sync(native, timeout_ms, tpl_ptr)

        Rdkafka::RdkafkaError.validate!(error_ptr)

        native_tpl = tpl_ptr.read_pointer

        next nil if native_tpl.null?

        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native_tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_tpl)
        end
      end
    end

    # Sends all pending acknowledgements to the broker without waiting for the responses.
    #
    # Outcomes are reported through the callback set with {#acknowledgement_commit_callback=}
    # once the broker replies arrive (serviced by a later {#poll}).
    #
    # @return [nil]
    # @raise [RdkafkaError] When enqueuing the commit fails
    def commit_async
      with_native(__method__) do |native|
        error_ptr = Rdkafka::Bindings.rd_kafka_share_commit_async(native)

        Rdkafka::RdkafkaError.validate!(error_ptr)
      end

      nil
    end

    # Sets the callback invoked with the outcome of acknowledgement commits, once per partition
    # per commit, from within {#poll}/{#commit_sync}/{#commit_async} on the consumer thread.
    #
    # The callable is invoked with two arguments:
    # - an Array of Hashes `{ topic: String, partition: Integer, offsets: Array<Integer> }`
    #   describing the acknowledged offsets
    # - an RdkafkaError or nil with the outcome for those offsets
    #
    # @param callback [Proc, #call, nil] callable object or nil to clear the callback
    # @return [nil]
    # @raise [TypeError] When the callback is not callable
    # @raise [RdkafkaError] When (de)registering the callback fails
    #
    # @note No share consumer methods may be called from within the callback (librdkafka rejects
    #   re-entry). Exceptions raised by the callback are logged and swallowed, mirroring the
    #   producer delivery callback behavior.
    def acknowledgement_commit_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.nil? || callback.respond_to?(:call)

      function = callback && self.class.build_acknowledgement_commit_function(callback)

      with_native(__method__) do |native|
        error_ptr = Rdkafka::Bindings.rd_kafka_share_set_acknowledgement_commit_cb(
          native,
          function,
          FFI::Pointer::NULL
        )

        Rdkafka::RdkafkaError.validate!(error_ptr)

        # Retain the function only after successful registration, in the state holder shared
        # with the finalizer: the finalizer-driven destroy can still invoke it while flushing
        # pending acknowledgements, so it must outlive the consumer object itself
        @state.ack_callback = function
      end
    end

    # Builds the native acknowledgement commit callback function wrapping the given callable.
    #
    # Built in a class method on purpose: a Ruby block always closes over its `self`, and an
    # instance-level block would chain finalizer -> state -> function -> block -> consumer,
    # making a consumer with a registered callback permanently uncollectable. Here the captured
    # `self` is the class and the block holds only the user callback.
    #
    # @private
    # @param callback [#call] user callable
    # @return [FFI::Function] native callback function
    def self.build_acknowledgement_commit_function(callback)
      FFI::Function.new(
        :void, [:pointer, :pointer, :int, :pointer]
      ) do |_share_ptr, list_ptr, err_code, _opaque|
        callback.call(
          partition_offsets_from_native(list_ptr),
          Rdkafka::RdkafkaError.build(err_code) || nil
        )
      rescue Exception => err
        Rdkafka::Config.logger.error("Unhandled exception in acknowledgement commit callback: #{err.class} - #{err.message}")
      end
    end

    # Converts a native rd_kafka_share_partition_offsets_list_t into an Array of Hashes. The
    # native list is owned by librdkafka for the duration of the callback and must not be
    # retained or destroyed.
    #
    # @private
    # @param list_ptr [FFI::Pointer] native partition offsets list
    # @return [Array<Hash>] one Hash per partition with :topic, :partition and :offsets keys
    def self.partition_offsets_from_native(list_ptr)
      count = Rdkafka::Bindings.rd_kafka_share_partition_offsets_list_count(list_ptr)

      Array.new(count) do |i|
        entry_ptr = Rdkafka::Bindings.rd_kafka_share_partition_offsets_list_get(list_ptr, i)
        partition = Rdkafka::Bindings::TopicPartition.new(
          Rdkafka::Bindings.rd_kafka_share_partition_offsets_partition(entry_ptr)
        )
        offsets_cnt = Rdkafka::Bindings.rd_kafka_share_partition_offsets_offsets_cnt(entry_ptr)
        offsets_ptr = Rdkafka::Bindings.rd_kafka_share_partition_offsets_offsets(entry_ptr)

        {
          topic: partition[:topic],
          partition: partition[:partition],
          offsets: offsets_ptr.null? ? [] : offsets_ptr.read_array_of_int64(offsets_cnt)
        }
      end
    end

    # Poll for messages repeatedly and yield them one by one. Iteration ends when the consumer
    # is closed.
    #
    # As with {#poll}, an entry that fails to build is yielded as an `RdkafkaError` rather than a
    # {ShareConsumer::Message}, so a block that touches message accessors should guard accordingly.
    #
    # @yield [message] a polled entry
    # @yieldparam message [ShareConsumer::Message, RdkafkaError]
    # @return [nil]
    # @raise [RdkafkaError] When polling fails
    def each
      loop do
        # Only a ClosedConsumerError raised by poll itself ends iteration; the yield is kept
        # outside the rescue so an error raised from the caller's block propagates instead of
        # being mistaken for the consumer closing.
        batch =
          begin
            poll
          rescue Rdkafka::ClosedConsumerError
            break
          end

        batch.each { |message| yield(message) }
      end
    end

    # Marks failed oauth token acquire in librdkafka for this share consumer
    #
    # @param reason [String] human readable error reason for failing to acquire token
    def oauthbearer_set_token_failure(reason)
      with_native(__method__) do |native|
        Rdkafka::Bindings.rd_kafka_share_oauthbearer_set_token_failure(native, reason)
      end
    end

    # Whether this share consumer has closed. Also reports closed in any process other than the
    # one that created it: librdkafka is not fork-safe, so an inherited handle must be treated as
    # unusable (and never torn down) in a forked child.
    # @return [Boolean]
    def closed?
      @state.native.nil? || @state.creator_pid != Process.pid
    end

    # Closes this share consumer: sends any pending acknowledgements, leaves the share group and
    # frees the native handle. Waits for in-flight calls from other threads to finish first.
    # Close is bounded internally by `socket.timeout.ms` (there is no timeout argument in the
    # librdkafka preview).
    #
    # @return [nil]
    # @raise [RdkafkaError] When closing or destroying reported an error. librdkafka leaves the
    #   instance intact in that case, so the consumer keeps its native handle and `close` can be
    #   retried (for example after returning from the acknowledgement commit callback, from
    #   within which closing is rejected).
    def close
      return if closed?

      @access_mutex.synchronize do
        # Wait for in-flight calls on other threads to finish using the handle. New calls block
        # on the mutex above while we hold it.
        sleep(Defaults::NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS / 1_000.0) until @operations_in_progress.zero?

        native = @state.native

        # Lost the race against a concurrent close that already finished
        return if native.nil?

        # librdkafka leaves the instance intact when close/destroy fail, and validate! raises
        # before any reference (or the finalizer) is dropped, so the caller can retry close
        # and the handle cannot leak silently.
        #
        # The close step is skipped when librdkafka already considers the consumer closed
        # (e.g. a prior close attempt succeeded but its destroy failed and was retried).
        if Rdkafka::Bindings.rd_kafka_share_consumer_closed(native).zero?
          Rdkafka::RdkafkaError.validate!(Rdkafka::Bindings.rd_kafka_share_consumer_close(native))
        end

        Rdkafka::RdkafkaError.validate!(Rdkafka::Bindings.rd_kafka_share_destroy(native))

        # The native handle is gone only now; drop all references and the finalizer
        ObjectSpace.undefine_finalizer(self)
        @state.native = nil
        @state.ack_callback = nil
      end

      nil
    end

    private

    # Executes a block with the native share handle while tracking the call so {#close} cannot
    # tear the handle down mid-call from another thread. Increments happen under the access
    # mutex that close holds for the whole teardown, so once close is draining no new call can
    # slip in; librdkafka's own single-owner gate still rejects genuinely concurrent API calls.
    #
    # @param method [Symbol] public method name, used for the ClosedConsumerError
    # @yield [FFI::Pointer] the native share handle
    # @raise [ClosedConsumerError] when the consumer is closed or closing
    def with_native(method)
      # Owning the mutex here means re-entry from a callback fired during #close's teardown
      # (the only internal holder) - the handle is going away, so reject before tracking
      raise Rdkafka::ClosedConsumerError.new(method) if @access_mutex.owned?

      @access_mutex.synchronize { @operations_in_progress += 1 }

      begin
        native = @state.native

        # nil handle => closed; different pid => inherited by a forked child, where librdkafka is
        # not fork-safe and the handle must not be used.
        raise Rdkafka::ClosedConsumerError.new(method) if native.nil? || @state.creator_pid != Process.pid

        yield(native)
      ensure
        @decrement_mutex.synchronize { @operations_in_progress -= 1 }
      end
    end

    # Performs the share-specific native token set call, reusing the buffer and extension
    # plumbing from {Helpers::OAuth}
    #
    # @param token [String] the token value
    # @param lifetime_ms [Integer] token expiry, in milliseconds since the epoch
    # @param principal_name [String] the Kafka principal name associated with the token
    # @param extensions_ptr [FFI::Pointer, nil] `const char **` built by
    #   {Helpers::OAuth#map_extensions}
    # @param extensions_size [Integer] number of key-value pairs pointed to by `extensions_ptr`
    # @param error_buffer [FFI::MemoryPointer] 256-byte buffer for the error string
    # @return [Integer] 0 on success
    def oauthbearer_native_set_token(token, lifetime_ms, principal_name, extensions_ptr, extensions_size, error_buffer)
      with_native(:oauthbearer_set_token) do |native|
        Rdkafka::Bindings.rd_kafka_share_oauthbearer_set_token(
          native, token, lifetime_ms, principal_name,
          extensions_ptr, extensions_size, error_buffer, 256
        )
      end
    end
  end
end

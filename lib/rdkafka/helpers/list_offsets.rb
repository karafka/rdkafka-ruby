# frozen_string_literal: true

module Rdkafka
  module Helpers
    # Shared `#list_offsets` implementation for Admin and Consumer.
    #
    # The librdkafka Admin API is handle-agnostic: `rd_kafka_ListOffsets()` can be issued on any
    # `rd_kafka_t` instance regardless of its type, so this Ruby-level implementation is identical
    # for admin and consumer clients. Includers must provide a private `#closed_check(method)`
    # that raises their own `Closed*Error`, and must have the background event callback
    # registered on their native config (see `Config#admin` and `Config#consumer`) so the result
    # event is dispatched back to the returned handle.
    module ListOffsets
      # Queries partition offsets by specification (earliest, latest, max_timestamp, or by
      # timestamp) without requiring a consumer group.
      #
      # The query is batched: all requested partitions are carried in one `ListOffsets` request
      # that librdkafka fans out to the involved partition leaders internally and concurrently.
      #
      # @param topic_partition_offsets [Hash{String => Array<Hash>}] hash mapping topic names to
      #   arrays of partition offset specifications. Each specification is a hash with:
      #   - `:partition` [Integer] partition number
      #   - `:offset` [Symbol, Integer] offset specification - `:earliest`, `:latest`,
      #     `:max_timestamp`, or an integer timestamp in milliseconds
      # @param isolation_level [Integer, nil] optional isolation level:
      #   - `RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED` (0) - default
      #   - `RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED` (1)
      #
      # @return [Admin::ListOffsetsHandle] handle that can be used to wait for the result
      #
      # @raise [ClosedAdminError, ClosedConsumerError] when the client is closed
      # @raise [ConfigError] when the background queue is unavailable
      #
      # @example Query earliest and latest offsets
      #   handle = client.list_offsets(
      #     { "my_topic" => [
      #       { partition: 0, offset: :earliest },
      #       { partition: 1, offset: :latest }
      #     ] }
      #   )
      #   report = handle.wait(max_wait_timeout_ms: 15_000)
      #   report.offsets
      #   # => [{ topic: "my_topic", partition: 0, offset: 0, ... }, ...]
      def list_offsets(topic_partition_offsets, isolation_level: nil)
        closed_check(__method__)

        # Parse and validate every offset spec before allocating the native list, so a missing key
        # or an unknown offset specification raises with nothing to clean up. Previously the
        # ArgumentError (or KeyError) was raised after `rd_kafka_topic_partition_list_new`, leaking
        # the native list.
        parsed = topic_partition_offsets.flat_map do |topic, partitions|
          partitions.map do |spec|
            offset = spec.fetch(:offset)

            native_offset = case offset
            when :earliest then Rdkafka::Bindings::RD_KAFKA_OFFSET_SPEC_EARLIEST
            when :latest then Rdkafka::Bindings::RD_KAFKA_OFFSET_SPEC_LATEST
            when :max_timestamp then Rdkafka::Bindings::RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP
            when Integer then offset
            else
              raise ArgumentError, "Unknown offset specification: #{offset.inspect}"
            end

            [topic, spec.fetch(:partition), native_offset]
          end
        end

        # Build native topic partition list
        tpl = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(parsed.size)

        parsed.each do |topic, partition, native_offset|
          Rdkafka::Bindings.rd_kafka_topic_partition_list_add(tpl, topic, partition)
          Rdkafka::Bindings.rd_kafka_topic_partition_list_set_offset(tpl, topic, partition, native_offset)
        end

        # Get a pointer to the queue that our request will be enqueued on
        queue_ptr = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
        end

        if queue_ptr.null?
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
          raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
        end

        # Create and register the handle we will return to the caller
        handle = Admin::ListOffsetsHandle.new
        handle[:pending] = true
        handle[:response] = Rdkafka::Bindings::RD_KAFKA_PARTITION_UA

        admin_options_ptr = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_AdminOptions_new(
            inner,
            Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_LISTOFFSETS
          )
        end

        if isolation_level
          Rdkafka::Bindings.rd_kafka_AdminOptions_set_isolation_level(admin_options_ptr, isolation_level)
        end

        Admin::ListOffsetsHandle.register(handle)
        Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, handle.to_ptr)

        begin
          @native_kafka.with_inner do |inner|
            Rdkafka::Bindings.rd_kafka_ListOffsets(
              inner,
              tpl,
              admin_options_ptr,
              queue_ptr
            )
          end
        rescue Exception
          Admin::ListOffsetsHandle.remove(handle.to_ptr.address)
          raise
        ensure
          Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
          Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
        end

        handle
      end
    end
  end
end

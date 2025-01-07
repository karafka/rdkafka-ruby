# frozen_string_literal: true

module Rdkafka
  # A consumer of Kafka messages. It uses the high-level consumer approach where the Kafka
  # brokers automatically assign partitions and load balance partitions over consumers that
  # have the same `:"group.id"` set in their configuration.
  #
  # To create a consumer set up a {Config} and call {Config#consumer consumer} on that. It is
  # mandatory to set `:"group.id"` in the configuration.
  #
  # Consumer implements `Enumerable`, so you can use `each` to consume messages, or for example
  # `each_slice` to consume batches of messages.
  class Consumer
    include Enumerable
    include Helpers::Time
    include Helpers::OAuth

    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka
    end

    # Starts the native Kafka polling thread and kicks off the init polling
    # @note Not needed to run unless explicit start was disabled
    def start
      @native_kafka.start
    end

    # @return [String] consumer name
    def name
      @name ||= @native_kafka.with_inner do |inner|
        ::Rdkafka::Bindings.rd_kafka_name(inner)
      end
    end

    def finalizer
      ->(_) { close }
    end

    # Close this consumer
    # @return [nil]
    def close
      return if closed?
      ObjectSpace.undefine_finalizer(self)

      @native_kafka.synchronize do |inner|
        Rdkafka::Bindings.rd_kafka_consumer_close(inner)
      end

      @native_kafka.close
    end

    # Whether this consumer has closed
    def closed?
      @native_kafka.closed?
    end

    # Subscribes to one or more topics letting Kafka handle partition assignments.
    #
    # @param topics [Array<String>] One or more topic names
    # @return [nil]
    # @raise [RdkafkaError] When subscribing fails
    def subscribe(*topics)
      closed_consumer_check(__method__)

      # Create topic partition list with topics and no partition set
      tpl = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(topics.length)

      topics.each do |topic|
        Rdkafka::Bindings.rd_kafka_topic_partition_list_add(tpl, topic, -1)
      end

      # Subscribe to topic partition list and check this was successful
      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_subscribe(inner, tpl)
      end

      Rdkafka::RdkafkaError.validate!(response, "Error subscribing to '#{topics.join(', ')}'")
    ensure
      Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl) unless tpl.nil?
    end

    # Unsubscribe from all subscribed topics.
    #
    # @return [nil]
    # @raise [RdkafkaError] When unsubscribing fails
    def unsubscribe
      closed_consumer_check(__method__)

      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_unsubscribe(inner)
      end

      Rdkafka::RdkafkaError.validate!(response)

      nil
    end

    # Pause producing or consumption for the provided list of partitions
    #
    # @param list [TopicPartitionList] The topic with partitions to pause
    # @return [nil]
    # @raise [RdkafkaTopicPartitionListError] When pausing subscription fails.
    def pause(list)
      closed_consumer_check(__method__)

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_pause_partitions(inner, tpl)
        end

        if response != 0
          list = TopicPartitionList.from_native_tpl(tpl)
          raise Rdkafka::RdkafkaTopicPartitionListError.new(response, list, "Error pausing '#{list.to_h}'")
        end
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Resumes producing consumption for the provided list of partitions
    #
    # @param list [TopicPartitionList] The topic with partitions to pause
    # @return [nil]
    # @raise [RdkafkaError] When resume subscription fails.
    def resume(list)
      closed_consumer_check(__method__)

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_resume_partitions(inner, tpl)
        end

        Rdkafka::RdkafkaError.validate!(response, "Error resume '#{list.to_h}'")

        nil
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Returns the current subscription to topics and partitions
    #
    # @return [TopicPartitionList]
    # @raise [RdkafkaError] When getting the subscription fails.
    def subscription
      closed_consumer_check(__method__)

      ptr = FFI::MemoryPointer.new(:pointer)
      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_subscription(inner, ptr)
      end

      Rdkafka::RdkafkaError.validate!(response)

      native = ptr.read_pointer

      begin
        Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native)
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native)
      end
    end

    # Atomic assignment of partitions to consume
    #
    # @param list [TopicPartitionList] The topic with partitions to assign
    # @raise [RdkafkaError] When assigning fails
    def assign(list)
      closed_consumer_check(__method__)

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_assign(inner, tpl)
        end

        Rdkafka::RdkafkaError.validate!(response, "Error assigning '#{list.to_h}'")
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Returns the current partition assignment.
    #
    # @return [TopicPartitionList]
    # @raise [RdkafkaError] When getting the assignment fails.
    def assignment
      closed_consumer_check(__method__)

      ptr = FFI::MemoryPointer.new(:pointer)
      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_assignment(inner, ptr)
      end

      Rdkafka::RdkafkaError.validate!(response)

      tpl = ptr.read_pointer

      if !tpl.null?
        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy tpl
        end
      end
    ensure
      ptr.free unless ptr.nil?
    end

    # @return [Boolean] true if our current assignment has been lost involuntarily.
    def assignment_lost?
      closed_consumer_check(__method__)

      @native_kafka.with_inner do |inner|
        !Rdkafka::Bindings.rd_kafka_assignment_lost(inner).zero?
      end
    end

    # Return the current committed offset per partition for this consumer group.
    # The offset field of each requested partition will either be set to stored offset or to -1001
    # in case there was no stored offset for that partition.
    #
    # @param list [TopicPartitionList, nil] The topic with partitions to get the offsets for or nil
    #   to use the current subscription.
    # @param timeout_ms [Integer] The timeout for fetching this information.
    # @return [TopicPartitionList]
    # @raise [RdkafkaError] When getting the committed positions fails.
    def committed(list=nil, timeout_ms=2_000)
      closed_consumer_check(__method__)

      if list.nil?
        list = assignment
      elsif !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be nil or a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_committed(inner, tpl, timeout_ms)
        end

        Rdkafka::RdkafkaError.validate!(response)

        TopicPartitionList.from_native_tpl(tpl)
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Return the current positions (offsets) for topics and partitions.
    # The offset field of each requested partition will be set to the offset of the last consumed message + 1, or nil in case there was no previous message.
    #
    # @param list [TopicPartitionList, nil] The topic with partitions to get the offsets for or nil to use the current subscription.
    #
    # @return [TopicPartitionList]
    #
    # @raise [RdkafkaError] When getting the positions fails.
    def position(list=nil)
      if list.nil?
        list = assignment
      elsif !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be nil or a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_position(inner, tpl)
      end

      Rdkafka::RdkafkaError.validate!(response)

      TopicPartitionList.from_native_tpl(tpl)
    end

    # Query broker for low (oldest/beginning) and high (newest/end) offsets for a partition.
    #
    # @param topic [String] The topic to query
    # @param partition [Integer] The partition to query
    # @param timeout_ms [Integer] The timeout for querying the broker
    # @return [Integer] The low and high watermark
    # @raise [RdkafkaError] When querying the broker fails.
    def query_watermark_offsets(topic, partition, timeout_ms=1000)
      closed_consumer_check(__method__)

      low = FFI::MemoryPointer.new(:int64, 1)
      high = FFI::MemoryPointer.new(:int64, 1)

      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_query_watermark_offsets(
          inner,
          topic,
          partition,
          low,
          high,
          timeout_ms,
        )
      end

      Rdkafka::RdkafkaError.validate!(response, "Error querying watermark offsets for partition #{partition} of #{topic}")

      return low.read_array_of_int64(1).first, high.read_array_of_int64(1).first
    ensure
      low.free   unless low.nil?
      high.free  unless high.nil?
    end

    # Calculate the consumer lag per partition for the provided topic partition list.
    # You can get a suitable list by calling {committed} or {position} (TODO). It is also
    # possible to create one yourself, in this case you have to provide a list that
    # already contains all the partitions you need the lag for.
    #
    # @param topic_partition_list [TopicPartitionList] The list to calculate lag for.
    # @param watermark_timeout_ms [Integer] The timeout for each query watermark call.
    # @return [Hash<String, Hash<Integer, Integer>>] A hash containing all topics with the lag
    #   per partition
    # @raise [RdkafkaError] When querying the broker fails.
    def lag(topic_partition_list, watermark_timeout_ms=1000)
      out = {}

      topic_partition_list.to_h.each do |topic, partitions|
        # Query high watermarks for this topic's partitions
        # and compare to the offset in the list.
        topic_out = {}
        partitions.each do |p|
          next if p.offset.nil?
          _, high = query_watermark_offsets(
            topic,
            p.partition,
            watermark_timeout_ms
          )
          topic_out[p.partition] = high - p.offset
        end
        out[topic] = topic_out
      end
      out
    end

    # Returns the ClusterId as reported in broker metadata.
    #
    # @return [String, nil]
    def cluster_id
      closed_consumer_check(__method__)
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_clusterid(inner)
      end
    end

    # Returns this client's broker-assigned group member id
    #
    # This currently requires the high-level KafkaConsumer
    #
    # @return [String, nil]
    def member_id
      closed_consumer_check(__method__)
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_memberid(inner)
      end
    end

    # Store offset of a message to be used in the next commit of this consumer
    #
    # When using this `enable.auto.offset.store` should be set to `false` in the config.
    #
    # @param message [Rdkafka::Consumer::Message] The message which offset will be stored
    # @param metadata [String, nil] commit metadata string or nil if none
    # @return [nil]
    # @raise [RdkafkaError] When storing the offset fails
    def store_offset(message, metadata = nil)
      closed_consumer_check(__method__)

      list = TopicPartitionList.new

      # For metadata aware commits we build the partition reference directly to save on
      # objects allocations
      if metadata
        list.add_topic_and_partitions_with_offsets(
          message.topic,
          [
            Consumer::Partition.new(
              message.partition,
              message.offset + 1,
              0,
              metadata
            )
          ]
        )
      else
        list.add_topic_and_partitions_with_offsets(
          message.topic,
          message.partition => message.offset + 1
        )
      end

      tpl = list.to_native_tpl

      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_offsets_store(
          inner,
          tpl
        )
      end

      Rdkafka::RdkafkaError.validate!(response)

      nil
    ensure
      Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl) if tpl
    end

    # Seek to a particular message. The next poll on the topic/partition will return the
    # message at the given offset.
    #
    # @param message [Rdkafka::Consumer::Message] The message to which to seek
    # @return [nil]
    # @raise [RdkafkaError] When seeking fails
    def seek(message)
      seek_by(message.topic, message.partition, message.offset)
    end

    # Seek to a particular message by providing the topic, partition and offset.
    # The next poll on the topic/partition will return the
    # message at the given offset.
    #
    # @param topic [String] The topic in which to seek
    # @param partition [Integer] The partition number to seek
    # @param offset [Integer] The partition offset to seek
    # @return [nil]
    # @raise [RdkafkaError] When seeking fails
    def seek_by(topic, partition, offset)
      closed_consumer_check(__method__)

      # rd_kafka_offset_store is one of the few calls that does not support
      # a string as the topic, so create a native topic for it.
      native_topic = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_topic_new(
          inner,
          topic,
          nil
        )
      end
      response = Rdkafka::Bindings.rd_kafka_seek(
        native_topic,
        partition,
        offset,
        0 # timeout
      )
      Rdkafka::RdkafkaError.validate!(response)

      nil
    ensure
      if native_topic && !native_topic.null?
        Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic)
      end
    end

    # Lookup offset for the given partitions by timestamp.
    #
    # @param list [TopicPartitionList] The TopicPartitionList with timestamps instead of offsets
    #
    # @return [TopicPartitionList]
    #
    # @raise [RdKafkaError] When the OffsetForTimes lookup fails
    def offsets_for_times(list, timeout_ms = 1000)
      closed_consumer_check(__method__)

      if !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_offsets_for_times(
          inner,
          tpl,
          timeout_ms # timeout
        )
      end

      Rdkafka::RdkafkaError.validate!(response)

      TopicPartitionList.from_native_tpl(tpl)
    ensure
      Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl) if tpl
    end

    # Manually commit the current offsets of this consumer.
    #
    # To use this set `enable.auto.commit`to `false` to disable automatic triggering
    # of commits.
    #
    # If `enable.auto.offset.store` is set to `true` the offset of the last consumed
    # message for every partition is used. If set to `false` you can use {store_offset} to
    # indicate when a message has been fully processed.
    #
    # @param list [TopicPartitionList,nil] The topic with partitions to commit
    # @param async [Boolean] Whether to commit async or wait for the commit to finish
    # @return [nil]
    # @raise [RdkafkaError] When committing fails
    def commit(list=nil, async=false)
      closed_consumer_check(__method__)

      if !list.nil? && !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be nil or a TopicPartitionList")
      end

      tpl = list ? list.to_native_tpl : nil

      begin
        response = @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_commit(inner, tpl, async)
        end

        Rdkafka::RdkafkaError.validate!(response)

        nil
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl) if tpl
      end
    end

    # Poll for the next message on one of the subscribed topics
    #
    # @param timeout_ms [Integer] Timeout of this poll
    # @return [Message, nil] A message or nil if there was no new message within the timeout
    # @raise [RdkafkaError] When polling fails
    def poll(timeout_ms)
      closed_consumer_check(__method__)

      message_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_consumer_poll(inner, timeout_ms)
      end

      return nil if message_ptr.null?

      # Create struct wrapper
      native_message = Rdkafka::Bindings::Message.new(message_ptr)

      # Create a message to pass out
      return Rdkafka::Consumer::Message.new(native_message) if native_message[:err].zero?

      # Raise error if needed
      Rdkafka::RdkafkaError.validate!(native_message)
    ensure
      # Clean up rdkafka message if there is one
      if message_ptr && !message_ptr.null?
        Rdkafka::Bindings.rd_kafka_message_destroy(message_ptr)
      end
    end

    # Polls the main rdkafka queue (not the consumer one). Do **NOT** use it if `consumer_poll_set`
    #   was set to `true`.
    #
    # Events will cause application-provided callbacks to be called.
    #
    # Events (in the context of the consumer):
    #   - error callbacks
    #   - stats callbacks
    #   - any other callbacks supported by librdkafka that are not part of the consumer_poll, that
    #     would have a callback configured and activated.
    #
    # This method needs to be called at regular intervals to serve any queued callbacks waiting to
    # be called. When in use, does **NOT** replace `#poll` but needs to run complementary with it.
    #
    # @param timeout_ms [Integer] poll timeout. If set to 0 will run async, when set to -1 will
    #   block until any events available.
    #
    # @note This method technically should be called `#poll` and the current `#poll` should be
    #   called `#consumer_poll` though we keep the current naming convention to make it backward
    #   compatible.
    def events_poll(timeout_ms = 0)
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_poll(inner, timeout_ms)
      end
    end

    # Poll for new messages and yield for each received one. Iteration
    # will end when the consumer is closed.
    #
    # If `enable.partition.eof` is turned on in the config this will raise an error when an eof is
    # reached, so you probably want to disable that when using this method of iteration.
    #
    # @yieldparam message [Message] Received message
    # @return [nil]
    # @raise [RdkafkaError] When polling fails
    def each
      loop do
        message = poll(250)
        if message
          yield(message)
        else
          if closed?
            break
          else
            next
          end
        end
      end
    end

    # Deprecated. Please read the error message for more details.
    def each_batch(max_items: 100, bytes_threshold: Float::INFINITY, timeout_ms: 250, yield_on_error: false, &block)
      raise NotImplementedError, <<~ERROR
        `each_batch` has been removed due to data consistency concerns.

        This method was removed because it did not properly handle partition reassignments,
        which could lead to processing messages from partitions that were no longer owned
        by this consumer, resulting in duplicate message processing and data inconsistencies.

        Recommended alternatives:

        1. Implement your own batching logic using rebalance callbacks to properly handle
           partition revocations and ensure message processing correctness.

        2. Use a high-level batching library that supports proper partition reassignment
           handling out of the box (such as the Karafka framework).
      ERROR
    end

    # Returns pointer to the consumer group metadata. It is used only in the context of
    # exactly-once-semantics in transactions, this is why it is never remapped to Ruby
    #
    # This API is **not** usable by itself from Ruby
    #
    # @note This pointer **needs** to be removed with `#rd_kafka_consumer_group_metadata_destroy`
    #
    # @private
    def consumer_group_metadata_pointer
      closed_consumer_check(__method__)

      @native_kafka.with_inner do |inner|
        Bindings.rd_kafka_consumer_group_metadata(inner)
      end
    end

    private

    def closed_consumer_check(method)
      raise Rdkafka::ClosedConsumerError.new(method) if closed?
    end
  end
end

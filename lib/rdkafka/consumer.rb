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

    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka
      @closing = false
    end

    # Close this consumer
    # @return [nil]
    def close
      return unless @native_kafka

      @closing = true
      Rdkafka::Bindings.rd_kafka_consumer_close(@native_kafka)
      Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
      @native_kafka = nil
    end

    # Subscribe to one or more topics letting Kafka handle partition assignments.
    #
    # @param topics [Array<String>] One or more topic names
    #
    # @raise [RdkafkaError] When subscribing fails
    #
    # @return [nil]
    def subscribe(*topics)
      raise "Illegal call to #subscribe after closing the consumer" if @native_kafka.nil?

      # Create topic partition list with topics and no partition set
      tpl = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(topics.length)

      topics.each do |topic|
        Rdkafka::Bindings.rd_kafka_topic_partition_list_add(tpl, topic, -1)
      end

      # Subscribe to topic partition list and check this was successful
      response = Rdkafka::Bindings.rd_kafka_subscribe(@native_kafka, tpl)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response, "Error subscribing to '#{topics.join(', ')}'")
      end
    ensure
      Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
    end

    # Unsubscribe from all subscribed topics.
    #
    # @raise [RdkafkaError] When unsubscribing fails
    #
    # @return [nil]
    def unsubscribe
      raise "Illegal call to #unsubscribe after closing the consumer" if @native_kafka.nil?

      response = Rdkafka::Bindings.rd_kafka_unsubscribe(@native_kafka)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end
    end

    # Pause producing or consumption for the provided list of partitions
    #
    # @param list [TopicPartitionList] The topic with partitions to pause
    #
    # @raise [RdkafkaTopicPartitionListError] When pausing subscription fails.
    #
    # @return [nil]
    def pause(list)
      raise "Illegal call to #pause after closing the consumer" if @native_kafka.nil?

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = Rdkafka::Bindings.rd_kafka_pause_partitions(@native_kafka, tpl)

        if response != 0
          list = TopicPartitionList.from_native_tpl(tpl)
          raise Rdkafka::RdkafkaTopicPartitionListError.new(response, list, "Error pausing '#{list.to_h}'")
        end
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Resume producing consumption for the provided list of partitions
    #
    # @param list [TopicPartitionList] The topic with partitions to pause
    #
    # @raise [RdkafkaError] When resume subscription fails.
    #
    # @return [nil]
    def resume(list)
      raise "Illegal call to #resume after closing the consumer" if @native_kafka.nil?

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = Rdkafka::Bindings.rd_kafka_resume_partitions(@native_kafka, tpl)
        if response != 0
          raise Rdkafka::RdkafkaError.new(response, "Error resume '#{list.to_h}'")
        end
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Return the current subscription to topics and partitions
    #
    # @raise [RdkafkaError] When getting the subscription fails.
    #
    # @return [TopicPartitionList]
    def subscription
      raise "Illegal call to #subscription after closing the consumer" if @native_kafka.nil?

      ptr = FFI::MemoryPointer.new(:pointer)
      response = Rdkafka::Bindings.rd_kafka_subscription(@native_kafka, ptr)

      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end

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
    #
    # @raise [RdkafkaError] When assigning fails
    def assign(list)
      raise "Illegal call to #assign after closing the consumer" if @native_kafka.nil?

      unless list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = Rdkafka::Bindings.rd_kafka_assign(@native_kafka, tpl)
        if response != 0
          raise Rdkafka::RdkafkaError.new(response, "Error assigning '#{list.to_h}'")
        end
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Returns the current partition assignment.
    #
    # @raise [RdkafkaError] When getting the assignment fails.
    #
    # @return [TopicPartitionList]
    def assignment
      raise "Illegal call to #assignment after closing the consumer" if @native_kafka.nil?

      ptr = FFI::MemoryPointer.new(:pointer)
      response = Rdkafka::Bindings.rd_kafka_assignment(@native_kafka, ptr)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end

      tpl = ptr.read_pointer

      if !tpl.null?
        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy tpl
        end
      end
    ensure
      ptr.free
    end

    # Return the current committed offset per partition for this consumer group.
    # The offset field of each requested partition will either be set to stored offset or to -1001 in case there was no stored offset for that partition.
    #
    # @param list [TopicPartitionList, nil] The topic with partitions to get the offsets for or nil to use the current subscription.
    # @param timeout_ms [Integer] The timeout for fetching this information.
    #
    # @raise [RdkafkaError] When getting the committed positions fails.
    #
    # @return [TopicPartitionList]
    def committed(list=nil, timeout_ms=1200)
      raise "Illegal call to #committed after closing the consumer" if @native_kafka.nil?

      if list.nil?
        list = assignment
      elsif !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be nil or a TopicPartitionList")
      end

      tpl = list.to_native_tpl

      begin
        response = Rdkafka::Bindings.rd_kafka_committed(@native_kafka, tpl, timeout_ms)
        if response != 0
          raise Rdkafka::RdkafkaError.new(response)
        end
        TopicPartitionList.from_native_tpl(tpl)
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
      end
    end

    # Query broker for low (oldest/beginning) and high (newest/end) offsets for a partition.
    #
    # @param topic [String] The topic to query
    # @param partition [Integer] The partition to query
    # @param timeout_ms [Integer] The timeout for querying the broker
    #
    # @raise [RdkafkaError] When querying the broker fails.
    #
    # @return [Integer] The low and high watermark
    def query_watermark_offsets(topic, partition, timeout_ms=200)
      raise "Illegal call to #query_watermark_offsets after closing the consumer" if @native_kafka.nil?

      low = FFI::MemoryPointer.new(:int64, 1)
      high = FFI::MemoryPointer.new(:int64, 1)

      response = Rdkafka::Bindings.rd_kafka_query_watermark_offsets(
        @native_kafka,
        topic,
        partition,
        low,
        high,
        timeout_ms,
      )
      if response != 0
        raise Rdkafka::RdkafkaError.new(response, "Error querying watermark offsets for partition #{partition} of #{topic}")
      end

      return low.read_array_of_int64(1).first, high.read_array_of_int64(1).first
    ensure
      low.free
      high.free
    end

    # Calculate the consumer lag per partition for the provided topic partition list.
    # You can get a suitable list by calling {committed} or {position} (TODO). It is also
    # possible to create one yourself, in this case you have to provide a list that
    # already contains all the partitions you need the lag for.
    #
    # @param topic_partition_list [TopicPartitionList] The list to calculate lag for.
    # @param watermark_timeout_ms [Integer] The timeout for each query watermark call.
    #
    # @raise [RdkafkaError] When querying the broker fails.
    #
    # @return [Hash<String, Hash<Integer, Integer>>] A hash containing all topics with the lag per partition
    def lag(topic_partition_list, watermark_timeout_ms=100)
      out = {}

      topic_partition_list.to_h.each do |topic, partitions|
        # Query high watermarks for this topic's partitions
        # and compare to the offset in the list.
        topic_out = {}
        partitions.each do |p|
          next if p.offset.nil?
          low, high = query_watermark_offsets(
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
      raise "Illegal call to #cluster_id after closing the consumer" if @native_kafka.nil?
      Rdkafka::Bindings.rd_kafka_clusterid(@native_kafka)
    end

    # Returns this client's broker-assigned group member id
    #
    # This currently requires the high-level KafkaConsumer
    #
    # @return [String, nil]
    def member_id
      raise "Illegal call to #member_id after closing the consumer" if @native_kafka.nil?
      Rdkafka::Bindings.rd_kafka_memberid(@native_kafka)
    end

    # Store offset of a message to be used in the next commit of this consumer
    #
    # When using this `enable.auto.offset.store` should be set to `false` in the config.
    #
    # @param message [Rdkafka::Consumer::Message] The message which offset will be stored
    #
    # @raise [RdkafkaError] When storing the offset fails
    #
    # @return [nil]
    def store_offset(message)
      raise "Illegal call to #store_offset after closing the consumer" if @native_kafka.nil?

      # rd_kafka_offset_store is one of the few calls that does not support
      # a string as the topic, so create a native topic for it.
      native_topic = Rdkafka::Bindings.rd_kafka_topic_new(
        @native_kafka,
        message.topic,
        nil
      )
      response = Rdkafka::Bindings.rd_kafka_offset_store(
        native_topic,
        message.partition,
        message.offset
      )
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end
    ensure
      if native_topic && !native_topic.null?
        Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic)
      end
    end

    # Seek to a particular message. The next poll on the topic/partition will return the
    # message at the given offset.
    #
    # @param message [Rdkafka::Consumer::Message] The message to which to seek
    #
    # @raise [RdkafkaError] When seeking fails
    #
    # @return [nil]
    def seek(message)
      raise "Illegal call to #seek after closing the consumer" if @native_kafka.nil?

      # rd_kafka_offset_store is one of the few calls that does not support
      # a string as the topic, so create a native topic for it.
      native_topic = Rdkafka::Bindings.rd_kafka_topic_new(
        @native_kafka,
        message.topic,
        nil
      )
      response = Rdkafka::Bindings.rd_kafka_seek(
        native_topic,
        message.partition,
        message.offset,
        0 # timeout
      )
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end
    ensure
      if native_topic && !native_topic.null?
        Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic)
      end
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
    #
    # @raise [RdkafkaError] When committing fails
    #
    # @return [nil]
    def commit(list=nil, async=false)
      raise "Illegal call to #commit after closing the consumer" if @native_kafka.nil?

      if !list.nil? && !list.is_a?(TopicPartitionList)
        raise TypeError.new("list has to be nil or a TopicPartitionList")
      end

      tpl = list ? list.to_native_tpl : nil

      begin
        response = Rdkafka::Bindings.rd_kafka_commit(@native_kafka, tpl, async)
        if response != 0
          raise Rdkafka::RdkafkaError.new(response)
        end
      ensure
        Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl) if tpl
      end
    end

    # Poll for the next message on one of the subscribed topics
    #
    # @param timeout_ms [Integer] Timeout of this poll
    #
    # @raise [RdkafkaError] When polling fails
    #
    # @return [Message, nil] A message or nil if there was no new message within the timeout
    def poll(timeout_ms)
      raise "Illegal call to #poll after closing the consumer" if @native_kafka.nil?

      message_ptr = Rdkafka::Bindings.rd_kafka_consumer_poll(@native_kafka, timeout_ms)
      if message_ptr.null?
        nil
      else
        # Create struct wrapper
        native_message = Rdkafka::Bindings::Message.new(message_ptr)
        # Raise error if needed
        if native_message[:err] != 0
          raise Rdkafka::RdkafkaError.new(native_message[:err])
        end
        # Create a message to pass out
        Rdkafka::Consumer::Message.new(native_message)
      end
    ensure
      # Clean up rdkafka message if there is one
      if !message_ptr.nil? && !message_ptr.null?
        Rdkafka::Bindings.rd_kafka_message_destroy(message_ptr)
      end
    end

    # Poll for new messages and yield for each received one. Iteration
    # will end when the consumer is closed.
    #
    # If `enable.partition.eof` is turned on in the config this will raise an
    # error when an eof is reached, so you probably want to disable that when
    # using this method of iteration.
    #
    # @raise [RdkafkaError] When polling fails
    #
    # @yieldparam message [Message] Received message
    #
    # @return [nil]
    def each
      loop do
        message = poll(250)
        if message
          yield(message)
        else
          if @closing
            break
          else
            next
          end
        end
      end
    end
  end
end

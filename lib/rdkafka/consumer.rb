module Rdkafka
  # A consumer of Kafka messages. It uses the high-level consumer approach where the Kafka
  # brokers automatically assign partitions and load balance partitions over consumers that
  # have the same `:"group.id"` set in their configuration.
  #
  # To create a consumer set up a {Config} and call {Config#consumer consumer} on that. It is
  # mandatory to set `:"group.id"` in the configuration.
  class Consumer
    include Enumerable

    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka
    end

    # Close this consumer
    # @return [nil]
    def close
      Rdkafka::FFI.rd_kafka_consumer_close(@native_kafka)
    end

    # Subscribe to one or more topics
    #
    # @param topics [Array<String>] One or more topic names
    #
    # @raise [RdkafkaError] When subscribing fails
    #
    # @return [nil]
    def subscribe(*topics)
      # Create topic partition list with topics and no partition set
      tpl = Rdkafka::FFI.rd_kafka_topic_partition_list_new(topics.length)
      topics.each do |topic|
        Rdkafka::FFI.rd_kafka_topic_partition_list_add(
          tpl,
          topic,
          -1
        )
      end
      # Subscribe to topic partition list and check this was successful
      response = Rdkafka::FFI.rd_kafka_subscribe(@native_kafka, tpl)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end
    ensure
      # Clean up the topic partition list
      Rdkafka::FFI.rd_kafka_topic_partition_list_destroy(tpl)
    end

    # Commit the current offsets of this consumer
    #
    # @param async [Boolean] Whether to commit async or wait for the commit to finish
    #
    # @raise [RdkafkaError] When comitting fails
    #
    # @return [nil]
    def commit(async=false)
      response = Rdkafka::FFI.rd_kafka_commit(@native_kafka, nil, async)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
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
      message_ptr = Rdkafka::FFI.rd_kafka_consumer_poll(@native_kafka, timeout_ms)
      if message_ptr.null?
        nil
      else
        # Create struct wrapper
        native_message = Rdkafka::FFI::Message.new(message_ptr)
        # Raise error if needed
        if native_message[:err] != 0
          raise Rdkafka::RdkafkaError.new(native_message[:err])
        end
        # Create a message to pass out
        Rdkafka::Consumer::Message.new(native_message)
      end
    ensure
      # Clean up rdkafka message if there is one
      unless message_ptr.null?
        Rdkafka::FFI.rd_kafka_message_destroy(message_ptr)
      end
    end

    # Poll for new messages and yield for each received one
    #
    # @raise [RdkafkaError] When polling fails
    #
    # @yieldparam message [Message] Received message
    #
    # @return [nil]
    def each(&block)
      loop do
        message = poll(250)
        if message
          block.call(message)
        else
          next
        end
      end
    end
  end
end

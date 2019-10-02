require "rdkafka/metadata"

module Rdkafka
  module AdminOperations
    # Retrieves metadata for a topic
    #
    # @param topic [String] Topic name
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    #
    # @raise [RdkafkaError] When retrieve metadata fails
    #
    # @return [nil]
    def metadata_for(topic, timeout: 5)
      t_p = Rdkafka::Bindings.rd_kafka_topic_new(
        @native_kafka,
        topic,
        nil
      )

      metadata_p = Rdkafka::Bindings::PointerPtr.new
      end_time = Time.now + timeout
      while Time.now < end_time
        err = Rdkafka::Bindings.rd_kafka_metadata(
          @native_kafka, 0, t_p,
          metadata_p, 100
        )
        break if err == 0
        sleep 1 # Ruby sleep seems to allow librdkafka to work
      end
      raise Rdkafka::RdkafkaError.new(err) unless err == 0

      metadata = Rdkafka::Metadata.from_native(metadata_p[:value])
      return metadata.topics.first

    ensure
      if metadata_p && !metadata_p[:value].null?
        Rdkafka::Bindings.rd_kafka_metadata_destroy(metadata_p[:value])
      end
      Rdkafka::Bindings.rd_kafka_topic_destroy(t_p) if t_p
    end

    # Creates a topic.
    #
    # @param topic [String] Topic name
    # @param num_partitions [Integer,1] Optional number of paritions
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    # @param config [Hash<String,String>] Optional topic configuration
    #
    # @raise [RdkafkaError] When create topic fails
    #
    # @return [nil]
    def create_topic(topic, num_partitions: 1, replication_factor: 1, timeout: 5, config: {})
      # Construct array of NewTopic
      topics_p = ffi_array_of_pointers(1) do |_|
        n = Rdkafka::Bindings.rd_kafka_NewTopic_new(
          topic,
          num_partitions,
          replication_factor,
          nil, 0)
        config.each do |key, value|
          Rdkafka::Bindings.rd_kafka_NewTopic_set_config(n, key.to_s, value.to_s)
        end
        n
      end

      # Perform and wait for response
      event_p = do_admin_op(timeout: timeout) do |options, queue|
        Rdkafka::Bindings.rd_kafka_CreateTopics(
          @native_kafka,
          topics_p, 1,
          options, queue
        )
      end

      # Read response
      cnt_p = Rdkafka::Bindings::SizePtr.new
      ptr = Rdkafka::Bindings.rd_kafka_CreateTopics_result_topics(event_p, cnt_p)
      response = Rdkafka::Bindings::TopicResult.from_native_array(ptr, cnt_p[:value]).first
      response.raise_if_error

      return nil
    ensure
      Rdkafka::Bindings.rd_kafka_event_destroy(event_p) if event_p
      if topics_p
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy_array(topics_p, 1)
        topics_p.free
      end
    end

    # Deletes an existing topic
    #
    # @param topic [String] Topic name
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    #
    # @raise [RdkafkaError] When delete topic fails
    #
    # @return [nil]
    def delete_topic(topic, timeout: 5)
      del_topics_p = ffi_array_of_pointers(1) do |_|
        Rdkafka::Bindings.rd_kafka_DeleteTopic_new(topic)
      end

      event_p = do_admin_op(timeout: timeout) do |options, queue|
        Rdkafka::Bindings.rd_kafka_DeleteTopics(
          @native_kafka,
          del_topics_p, 1,
          options, queue
        )
      end

      cnt_p = Rdkafka::Bindings::SizePtr.new
      ptr = Rdkafka::Bindings.rd_kafka_DeleteTopics_result_topics(event_p, cnt_p)
      response = Rdkafka::Bindings::TopicResult.from_native_array(ptr, cnt_p[:value]).first
      response.raise_if_error

      return nil
    ensure
      Rdkafka::Bindings.rd_kafka_event_destroy(event_p) if event_p
      if del_topics_p
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy_array(del_topics_p, 1)
        del_topics_p.free
      end
    end

    # Creates partitions for an existing topic
    #
    # @param topic [String] Topic name
    # @param num_partitions [Integer] Desired number of paritions
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    #
    # @raise [RdkafkaError] When create partitions fails
    #
    # @return [nil]
    def create_partitions_for(topic, num_partitions:, timeout: 5)
      new_parts_p = ffi_array_of_pointers(1) do |_|
        Rdkafka::Bindings.rd_kafka_NewPartitions_new(topic, num_partitions, nil, 0)
      end

      event_p = do_admin_op(timeout: timeout) do |options, queue|
        Rdkafka::Bindings.rd_kafka_CreatePartitions(
          @native_kafka,
          new_parts_p, 1,
          options, queue
        )
      end

      cnt_p = Rdkafka::Bindings::SizePtr.new
      ptr = Rdkafka::Bindings.rd_kafka_CreatePartitions_result_topics(event_p, cnt_p)
      response = Rdkafka::Bindings::TopicResult.from_native_array(ptr, cnt_p[:value]).first
      response.raise_if_error

      return nil
    ensure
      Rdkafka::Bindings.rd_kafka_event_destroy(event_p) if event_p
      if new_parts_p
        Rdkafka::Bindings.rd_kafka_NewPartitions_destroy_array(new_parts_p, 1)
        new_parts_p.free
      end
    end

    # Adjust the configuration of an existing topic.
    #
    # @param topic [String] Topic name
    # @param config [Hash<String,String>] Configuration key-values to adjust
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    #
    # @raise [RdkafkaError] When alter topic fails
    #
    # @return [nil]
    def alter_topic(topic, config, timeout: 5)
      configs_p = ffi_array_of_pointers(1) do |_|
        n = Rdkafka::Bindings.rd_kafka_ConfigResource_new(
          Rdkafka::Bindings::ResourceType[:RD_KAFKA_RESOURCE_TOPIC],
          topic
        )
        config.each do |key, value|
          Rdkafka::Bindings.rd_kafka_ConfigResource_set_config(n, key.to_s, value.to_s)
        end
        n
      end

      event_p = do_admin_op(timeout: timeout) do |options, queue|
        Rdkafka::Bindings.rd_kafka_AlterConfigs(
          @native_kafka,
          configs_p, 1,
          options, queue
        )
      end

      cnt_p = Rdkafka::Bindings::SizePtr.new
      ptr = Rdkafka::Bindings.rd_kafka_AlterConfigs_result_resources(event_p, cnt_p)
      response = Rdkafka::Bindings::ConfigResource.from_native_array(ptr, cnt_p[:value]).first
      response.raise_if_error

      return nil
    ensure
      Rdkafka::Bindings.rd_kafka_event_destroy(event_p) if event_p
      if configs_p
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy_array(configs_p, 1)
        configs_p.free
      end
    end

    # Retrieve the configuration of a topic.
    #
    # @param topic [String] Topic name
    # @param timeout [Integer,5] Optional timeout in seconds. Defaults to 5
    #
    # @raise [RdkafkaError] When describe topic fails
    #
    # @return [Hash<String, String>]
    def describe_topic(topic, timeout: 5)
      configs_p = ffi_array_of_pointers(1) do |_|
        Rdkafka::Bindings.rd_kafka_ConfigResource_new(
          Rdkafka::Bindings::ResourceType[:RD_KAFKA_RESOURCE_TOPIC],
          topic
        )
      end

      event_p = do_admin_op(timeout: timeout) do |options, queue|
        Rdkafka::Bindings.rd_kafka_DescribeConfigs(
          @native_kafka,
          configs_p, 1,
          options, queue
        )
      end

      cnt_p = Rdkafka::Bindings::SizePtr.new
      ptr = Rdkafka::Bindings.rd_kafka_DescribeConfigs_result_resources(event_p, cnt_p)
      response = Rdkafka::Bindings::ConfigResource.from_native_array(ptr, cnt_p[:value]).first
      response.raise_if_error

      return response.config
    ensure
      Rdkafka::Bindings.rd_kafka_event_destroy(event_p) if event_p
      if configs_p
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy_array(configs_p, 1)
        configs_p.free
      end
    end

  private
    def ffi_array_of_pointers(count)
      array = count.times.map do |x|
        yield x
      end
      array_p = FFI::MemoryPointer.new(:pointer, count)
      array_p.write_array_of_pointer(array)
      array_p
    end

    def do_admin_op(timeout: 5)
      queue = Rdkafka::Bindings.rd_kafka_queue_new(@native_kafka)

      options = Rdkafka::Bindings.rd_kafka_AdminOptions_new(
        @native_kafka,
        Rdkafka::Bindings::AdminOptions[:RD_KAFKA_ADMIN_OP_ANY]
      )
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_operation_timeout(options, timeout * 1000, nil, 0)

      yield(options, queue)

      end_time = Time.now + timeout
      while Time.now < end_time
        event = Rdkafka::Bindings.rd_kafka_queue_poll(queue, 100)
        return event unless event.null?
        sleep 1 # Ruby sleep seems to allow librdkafka to work
      end

      # If we reach here, we timed out
      # RD_KAFKA_RESP_ERR__TIMED_OUT = -185
      raise Rdkafka::RdkafkaError.new(-185)
    ensure
      Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(options) if options
      Rdkafka::Bindings.rd_kafka_queue_destroy(queue) if queue
    end
  end
end

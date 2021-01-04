module Rdkafka
  class Admin
    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka
      @closing = false

      # Start thread to poll client for callbacks
      @polling_thread = Thread.new do
        loop do
          Rdkafka::Bindings.rd_kafka_poll(@native_kafka, 250)
          # Exit thread if closing and the poll queue is empty
          if @closing && Rdkafka::Bindings.rd_kafka_outq_len(@native_kafka) == 0
            break
          end
        end
      end
      @polling_thread.abort_on_exception = true
    end

    # Close this admin instance
    def close
      return unless @native_kafka

      # Indicate to polling thread that we're closing
      @closing = true
      # Wait for the polling thread to finish up
      @polling_thread.join
      Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
      @native_kafka = nil
    end

    # Create a topic with the given partition count and replication factor
    #
    # @raise [ConfigError] When the partition count or replication factor are out of valid range
    # @raise [RdkafkaError] When the topic name is invalid or the topic already exists
    # @raise [RdkafkaError] When the topic configuration is invalid
    #
    # @return [CreateTopicHandle] Create topic handle that can be used to wait for the result of creating the topic
    def create_topic(topic_name, partition_count, replication_factor, topic_config={})

      # Create a rd_kafka_NewTopic_t representing the new topic
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      new_topic_ptr = Rdkafka::Bindings.rd_kafka_NewTopic_new(
        FFI::MemoryPointer.from_string(topic_name),
        partition_count,
        replication_factor,
        error_buffer,
        256
      )
      if new_topic_ptr.null?
        raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
      end

      unless topic_config.nil?
        topic_config.each do |key, value|
          Rdkafka::Bindings.rd_kafka_NewTopic_set_config(
            new_topic_ptr,
            key.to_s,
            value.to_s
          )
        end
      end

      # Note that rd_kafka_CreateTopics can create more than one topic at a time
      pointer_array = [new_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(@native_kafka)
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      create_topic_handle = CreateTopicHandle.new
      create_topic_handle[:pending] = true
      create_topic_handle[:response] = -1
      CreateTopicHandle.register(create_topic_handle)
      admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(@native_kafka, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATETOPICS)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_topic_handle.to_ptr)

      begin
        Rdkafka::Bindings.rd_kafka_CreateTopics(
            @native_kafka,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
        )
      rescue Exception => err
        CreateTopicHandle.remove(create_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
      end

      create_topic_handle
    end

    # Delete the named topic
    #
    # @raise [RdkafkaError] When the topic name is invalid or the topic does not exist
    #
    # @return [DeleteTopicHandle] Delete topic handle that can be used to wait for the result of deleting the topic
    def delete_topic(topic_name)

      # Create a rd_kafka_DeleteTopic_t representing the topic to be deleted
      delete_topic_ptr = Rdkafka::Bindings.rd_kafka_DeleteTopic_new(FFI::MemoryPointer.from_string(topic_name))

      # Note that rd_kafka_DeleteTopics can create more than one topic at a time
      pointer_array = [delete_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(@native_kafka)
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      delete_topic_handle = DeleteTopicHandle.new
      delete_topic_handle[:pending] = true
      delete_topic_handle[:response] = -1
      DeleteTopicHandle.register(delete_topic_handle)
      admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(@native_kafka, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETETOPICS)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_topic_handle.to_ptr)

      begin
        Rdkafka::Bindings.rd_kafka_DeleteTopics(
            @native_kafka,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
        )
      rescue Exception => err
        DeleteTopicHandle.remove(delete_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
      end

      delete_topic_handle
    end
  end
end

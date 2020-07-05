module Rdkafka
  module Callbacks

    # Extracts attributes of a rd_kafka_topic_result_t
    class TopicResult
      attr_reader :result_error, :error_string, :result_name

      def initialize(topic_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_topic_result_error(topic_result_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_topic_result_error_string(topic_result_pointer)
        @result_name = Rdkafka::Bindings.rd_kafka_topic_result_name(topic_result_pointer)
      end
    end

    BackgroundEventCallback = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, event_ptr, opaque_ptr|

      if Rdkafka::Bindings.rd_kafka_event_type(event_ptr) == Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT
        create_topics_result = Rdkafka::Bindings.rd_kafka_event_CreateTopics_result(event_ptr)

        # Get the number of create topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_topic_results = Rdkafka::Bindings.rd_kafka_CreateTopics_result_topics(create_topics_result, pointer_to_size_t)

        create_topic_results = (1..pointer_to_size_t.read_int).map do |index|
          pointer = (create_topic_results + (index - 1)).read_pointer
          TopicResult.new(pointer)
        end
        create_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_topic_handle = Rdkafka::Admin::CreateTopicHandle.remove(create_topic_handle_ptr.address)
          create_topic_handle[:response] = create_topic_results[0].result_error
          create_topic_handle[:error_string] = create_topic_results[0].error_string
          create_topic_handle[:result_name] = create_topic_results[0].result_name
          create_topic_handle[:pending] = false
        end
      elsif Rdkafka::Bindings.rd_kafka_event_type(event_ptr) == Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT
        delete_topics_result = Rdkafka::Bindings.rd_kafka_event_DeleteTopics_result(event_ptr)

        # Get the number of topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        delete_topic_results = Rdkafka::Bindings.rd_kafka_DeleteTopics_result_topics(delete_topics_result, pointer_to_size_t)

        delete_topic_results = (1..pointer_to_size_t.read_int).map do |index|
          pointer = (delete_topic_results + (index - 1)).read_pointer
          TopicResult.new(pointer)
        end
        delete_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if delete_topic_handle = Rdkafka::Admin::DeleteTopicHandle.remove(delete_topic_handle_ptr.address)
          delete_topic_handle[:response] = delete_topic_results[0].result_error
          delete_topic_handle[:error_string] = delete_topic_results[0].error_string
          delete_topic_handle[:result_name] = delete_topic_results[0].result_name
          delete_topic_handle[:pending] = false
        end
      else
        # NOP
      end
    end

    DeliveryCallback = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, message_ptr, opaque_ptr|
      message = Rdkafka::Bindings::Message.new(message_ptr)
      delivery_handle_ptr_address = message[:_private].address
      if delivery_handle = Rdkafka::Producer::DeliveryHandle.remove(delivery_handle_ptr_address)
        # Update delivery handle
        delivery_handle[:pending] = false
        delivery_handle[:response] = message[:err]
        delivery_handle[:partition] = message[:partition]
        delivery_handle[:offset] = message[:offset]
        # Call delivery callback on opaque
        if opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
          opaque.call_delivery_callback(Rdkafka::Producer::DeliveryReport.new(message[:partition], message[:offset], message[:err]))
        end
      end
    end
  end
end

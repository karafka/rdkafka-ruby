module Rdkafka
  module Callbacks

    # Extracts attributes of a rd_kafka_topic_result_t
    #
    # @private
    class TopicResult
      attr_reader :result_error, :error_string, :result_name

      def initialize(topic_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_topic_result_error(topic_result_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_topic_result_error_string(topic_result_pointer)
        @result_name = Rdkafka::Bindings.rd_kafka_topic_result_name(topic_result_pointer)
      end

      def self.create_topic_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # FFI Function used for Create Topic and Delete Topic callbacks
    BackgroundEventCallbackFunction = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, event_ptr, opaque_ptr|
      BackgroundEventCallback.call(client_ptr, event_ptr, opaque_ptr)
    end

    # @private
    class BackgroundEventCallback
      def self.call(_, event_ptr, _)
        event_type = Rdkafka::Bindings.rd_kafka_event_type(event_ptr)
        if event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT
          process_create_topic(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT
          process_delete_topic(event_ptr)
        end
      end

      private

      def self.process_create_topic(event_ptr)
        create_topics_result = Rdkafka::Bindings.rd_kafka_event_CreateTopics_result(event_ptr)

        # Get the number of create topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_topic_result_array = Rdkafka::Bindings.rd_kafka_CreateTopics_result_topics(create_topics_result, pointer_to_size_t)
        create_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, create_topic_result_array)
        create_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_topic_handle = Rdkafka::Admin::CreateTopicHandle.remove(create_topic_handle_ptr.address)
          create_topic_handle[:response] = create_topic_results[0].result_error
          create_topic_handle[:error_string] = create_topic_results[0].error_string
          create_topic_handle[:result_name] = create_topic_results[0].result_name
          create_topic_handle[:pending] = false
        end
      end

      def self.process_delete_topic(event_ptr)
        delete_topics_result = Rdkafka::Bindings.rd_kafka_event_DeleteTopics_result(event_ptr)

        # Get the number of topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        delete_topic_result_array = Rdkafka::Bindings.rd_kafka_DeleteTopics_result_topics(delete_topics_result, pointer_to_size_t)
        delete_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, delete_topic_result_array)
        delete_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if delete_topic_handle = Rdkafka::Admin::DeleteTopicHandle.remove(delete_topic_handle_ptr.address)
          delete_topic_handle[:response] = delete_topic_results[0].result_error
          delete_topic_handle[:error_string] = delete_topic_results[0].error_string
          delete_topic_handle[:result_name] = delete_topic_results[0].result_name
          delete_topic_handle[:pending] = false
        end
      end
    end

    # FFI Function used for Message Delivery callbacks

    DeliveryCallbackFunction = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, message_ptr, opaque_ptr|
      DeliveryCallback.call(client_ptr, message_ptr, opaque_ptr)
    end

    # @private
    class DeliveryCallback
      def self.call(_, message_ptr, opaque_ptr)
        message = Rdkafka::Bindings::Message.new(message_ptr)
        delivery_handle_ptr_address = message[:_private].address
        if delivery_handle = Rdkafka::Producer::DeliveryHandle.remove(delivery_handle_ptr_address)
          # Update delivery handle
          delivery_handle[:response] = message[:err]
          delivery_handle[:partition] = message[:partition]
          delivery_handle[:offset] = message[:offset]
          delivery_handle[:pending] = false
          # Call delivery callback on opaque
          if opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
            opaque.call_delivery_callback(Rdkafka::Producer::DeliveryReport.new(message[:partition], message[:offset], message[:err]))
          end
        end
      end
    end
  end
end

# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_CREATETOPICS_RESULT` events
    # @private
    class CreateTopicHandler < BaseHandler
      # Resolves the create-topic handle from its result event
      # @param event_ptr [FFI::Pointer] pointer to the event
      # @return [void]
      def self.call(event_ptr)
        create_topics_result = Rdkafka::Bindings.rd_kafka_event_CreateTopics_result(event_ptr)

        # Get the number of create topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_topic_result_array = Rdkafka::Bindings.rd_kafka_CreateTopics_result_topics(create_topics_result, pointer_to_size_t)
        create_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, create_topic_result_array)
        create_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_topic_handle = Rdkafka::Admin::CreateTopicHandle.remove(create_topic_handle_ptr.address)
          create_topic_handle[:response] = create_topic_results[0].result_error
          create_topic_handle.result = Rdkafka::Admin::CreateTopicReport.new(
            create_topic_results[0].error_string,
            create_topic_results[0].result_name
          )
          create_topic_handle.broker_message = create_topic_handle.result.error_string

          create_topic_handle.unlock
        end
      end
    end
  end
end

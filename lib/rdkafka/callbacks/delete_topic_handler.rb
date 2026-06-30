# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DELETETOPICS_RESULT` events
    # @private
    class DeleteTopicHandler < BaseHandler
      class << self
        # Resolves the delete-topic handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          delete_topics_result = Rdkafka::Bindings.rd_kafka_event_DeleteTopics_result(event_ptr)

          # Get the number of topic results
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          delete_topic_result_array = Rdkafka::Bindings.rd_kafka_DeleteTopics_result_topics(delete_topics_result, pointer_to_size_t)
          delete_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, delete_topic_result_array)
          delete_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if delete_topic_handle = Rdkafka::Admin::DeleteTopicHandle.remove(delete_topic_handle_ptr.address)
            unless resolve_operation_error(event_ptr, delete_topic_handle)
              delete_topic_handle[:response] = delete_topic_results[0].result_error
              delete_topic_handle.result = Rdkafka::Admin::DeleteTopicReport.new(
                delete_topic_results[0].error_string,
                delete_topic_results[0].result_name
              )
              delete_topic_handle.broker_message = delete_topic_handle.result.error_string

              delete_topic_handle.unlock
            end
          end
        end
      end
    end
  end
end

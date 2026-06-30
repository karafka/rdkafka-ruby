# frozen_string_literal: true

module Rdkafka
  # Callback handlers for librdkafka events
  # @private
  module Callbacks
    # Extracts attributes of a rd_kafka_topic_result_t
    #
    # @private
    class TopicResult
      attr_reader :result_error, :error_string, :result_name

      # @param topic_result_pointer [FFI::Pointer] pointer to the topic result struct
      def initialize(topic_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_topic_result_error(topic_result_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_topic_result_error_string(topic_result_pointer)
        @result_name = Rdkafka::Bindings.rd_kafka_topic_result_name(topic_result_pointer)
      end

      # @param count [Integer] number of results
      # @param array_pointer [FFI::Pointer] pointer to the results array
      # @return [Array<TopicResult>] array of topic results
      def self.create_topic_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # Extracts attributes of rd_kafka_group_result_t
    #
    # @private
    class GroupResult
      attr_reader :result_error, :error_string, :result_name

      # @param group_result_pointer [FFI::Pointer] pointer to the group result struct
      def initialize(group_result_pointer)
        native_error = Rdkafka::Bindings.rd_kafka_group_result_error(group_result_pointer)

        if native_error.null?
          @result_error = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          @error_string = FFI::Pointer::NULL
        else
          @result_error = native_error[:code]
          @error_string = native_error[:errstr]
        end

        @result_name = Rdkafka::Bindings.rd_kafka_group_result_name(group_result_pointer)
      end

      # @param count [Integer] number of results
      # @param array_pointer [FFI::Pointer] pointer to the results array
      # @return [Array<GroupResult>] array of group results
      def self.create_group_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # Extracts attributes of rd_kafka_acl_result_t
    #
    # @private
    class CreateAclResult
      attr_reader :result_error, :error_string

      # @param acl_result_pointer [FFI::Pointer] pointer to the ACL result struct
      def initialize(acl_result_pointer)
        rd_kafka_error_pointer = Bindings.rd_kafka_acl_result_error(acl_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_error_code(rd_kafka_error_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_error_string(rd_kafka_error_pointer)
      end

      # @param count [Integer] number of results
      # @param array_pointer [FFI::Pointer] pointer to the results array
      # @return [Array<CreateAclResult>] array of ACL results
      def self.create_acl_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # Extracts attributes of rd_kafka_DeleteAcls_result_response_t
    #
    # @private
    class DeleteAclResult
      attr_reader :result_error, :error_string, :matching_acls, :matching_acls_count

      # @param acl_result_pointer [FFI::Pointer] pointer to the delete ACL result response struct
      def initialize(acl_result_pointer)
        @matching_acls = []
        rd_kafka_error_pointer = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_response_error(acl_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_error_code(rd_kafka_error_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_error_string(rd_kafka_error_pointer)
        if @result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          # Get the number of matching acls
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          @matching_acls = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_response_matching_acls(acl_result_pointer, pointer_to_size_t)
          @matching_acls_count = pointer_to_size_t.read_int
        end
      end

      # @param count [Integer] number of results
      # @param array_pointer [FFI::Pointer] pointer to the results array
      # @return [Array<DeleteAclResult>] array of delete ACL results
      def self.delete_acl_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # Extracts attributes of rd_kafka_DeleteAcls_result_response_t
    #
    # @private
    class DescribeAclResult
      attr_reader :result_error, :error_string, :matching_acls, :matching_acls_count

      # @param event_ptr [FFI::Pointer] pointer to the event
      def initialize(event_ptr)
        @matching_acls = []
        @result_error = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)
        @error_string = Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr)
        if @result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          acl_describe_result = Rdkafka::Bindings.rd_kafka_event_DescribeAcls_result(event_ptr)
          # Get the number of matching acls
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          @matching_acls = Rdkafka::Bindings.rd_kafka_DescribeAcls_result_acls(acl_describe_result, pointer_to_size_t)
          @matching_acls_count = pointer_to_size_t.read_int
        end
      end
    end

    # Extracts attributes of rd_kafka_DescribeConfigs_result_t
    #
    # @private
    class DescribeConfigsResult
      attr_reader :result_error, :error_string, :results, :results_count

      # @param event_ptr [FFI::Pointer] pointer to the event
      def initialize(event_ptr)
        @results = []
        @result_error = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)
        @error_string = Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr)

        if @result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          configs_describe_result = Rdkafka::Bindings.rd_kafka_event_DescribeConfigs_result(event_ptr)
          # Get the number of matching acls
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          @results = Rdkafka::Bindings.rd_kafka_DescribeConfigs_result_resources(configs_describe_result, pointer_to_size_t)
          @results_count = pointer_to_size_t.read_int
        end
      end
    end

    # Extracts attributes of rd_kafka_IncrementalAlterConfigs_result_t
    #
    # @private
    class IncrementalAlterConfigsResult
      attr_reader :result_error, :error_string, :results, :results_count

      # @param event_ptr [FFI::Pointer] pointer to the event
      def initialize(event_ptr)
        @results = []
        @result_error = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)
        @error_string = Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr)

        if @result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          incremental_alter_result = Rdkafka::Bindings.rd_kafka_event_IncrementalAlterConfigs_result(event_ptr)
          # Get the number of matching acls
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          @results = Rdkafka::Bindings.rd_kafka_IncrementalAlterConfigs_result_resources(incremental_alter_result, pointer_to_size_t)
          @results_count = pointer_to_size_t.read_int
        end
      end
    end

    # Extracts attributes of rd_kafka_ListOffsets_result_t
    #
    # @private
    class ListOffsetsResult
      attr_reader :result_error, :error_string, :result_infos, :result_count

      # @param event_ptr [FFI::Pointer] pointer to the event
      def initialize(event_ptr)
        @result_infos = FFI::Pointer::NULL
        @result_error = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)
        @error_string = Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr)

        if @result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          list_offsets_result = Rdkafka::Bindings.rd_kafka_event_ListOffsets_result(event_ptr)
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          @result_infos = Rdkafka::Bindings.rd_kafka_ListOffsets_result_infos(list_offsets_result, pointer_to_size_t)
          @result_count = pointer_to_size_t.read(:size_t)
        end
      end
    end

    # @private
    class BackgroundEventCallback
      # Handles background events from librdkafka
      # @param _client_ptr [FFI::Pointer] unused client pointer
      # @param event_ptr [FFI::Pointer] pointer to the event
      # @param _opaque_ptr [FFI::Pointer] unused opaque pointer
      def self.call(_client_ptr, event_ptr, _opaque_ptr)
        handler = case Rdkafka::Bindings.rd_kafka_event_type(event_ptr)
        when Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT then CreateTopicHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT then DeleteTopicHandler
        when Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT then CreatePartitionsHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEGROUPS_RESULT then DeleteGroupsHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_CREATEACLS_RESULT then CreateAclHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEACLS_RESULT then DeleteAclHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT then DescribeAclHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT then DescribeConfigsHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT then IncrementalAlterConfigsHandler
        when Rdkafka::Bindings::RD_KAFKA_EVENT_LISTOFFSETS_RESULT then ListOffsetsHandler
        end

        handler&.call(event_ptr)
      ensure
        # Background queue events are owned by the application and must be destroyed once
        # served (see rd_kafka_conf_set_background_event_cb). All event-owned memory (result
        # arrays, strings) has to be copied into Ruby objects by the handlers above before this
        # runs - nothing may retain pointers into the event past this point.
        Rdkafka::Bindings.rd_kafka_event_destroy(event_ptr)
      end
    end

    # @private
    class DeliveryCallback
      # Handles message delivery callbacks
      # @param _client_ptr [FFI::Pointer] unused client pointer
      # @param message_ptr [FFI::Pointer] pointer to the delivered message
      # @param opaque_ptr [FFI::Pointer] pointer to the opaque object for callback context
      def self.call(_client_ptr, message_ptr, opaque_ptr)
        message = Rdkafka::Bindings::Message.new(message_ptr)
        delivery_handle_ptr_address = message[:_private].address
        if delivery_handle = Rdkafka::Producer::DeliveryHandle.remove(delivery_handle_ptr_address)
          topic_name = Rdkafka::Bindings.rd_kafka_topic_name(message[:rkt])

          # Update delivery handle
          delivery_handle[:response] = message[:err]
          delivery_handle[:partition] = message[:partition]
          delivery_handle[:offset] = message[:offset]
          # The topic is not stored on the handle here: it is already set as the handle's
          # `topic` Ruby attribute during produce, which spares a native string copy that
          # would otherwise be allocated and retained for every message.

          begin
            # Call delivery callback on opaque
            if opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
              opaque.call_delivery_callback(
                Rdkafka::Producer::DeliveryReport.new(
                  message[:partition],
                  message[:offset],
                  topic_name,
                  message[:err],
                  delivery_handle.label
                ),
                delivery_handle
              )
            end
          rescue Exception => err
            # A user delivery callback that raises must not escape this FFI callback: it runs on
            # librdkafka's polling thread (abort_on_exception = true), so an escaping exception
            # would crash the whole process. We log and swallow it, matching the rebalance
            # callback. The `ensure` below still unlocks the handle, so `wait` returns the
            # delivery report instead of blocking until its timeout and raising
            # WaitTimeoutError for a message that was in fact delivered.
            Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
          ensure
            delivery_handle.unlock
          end
        end
      end
    end

    # @private
    @@mutex = Mutex.new
    # @private
    @@current_pid = nil

    class << self
      # Defines or recreates after fork callbacks that require FFI thread so the callback thread
      # is always correctly initialized
      #
      # @see https://github.com/ffi/ffi/issues/1114
      def ensure_ffi_running
        @@mutex.synchronize do
          return if @@current_pid == ::Process.pid

          if const_defined?(:BackgroundEventCallbackFunction, false)
            send(:remove_const, :BackgroundEventCallbackFunction)
            send(:remove_const, :DeliveryCallbackFunction)
          end

          # FFI Function used for Create Topic and Delete Topic callbacks
          background_event_callback_function = FFI::Function.new(
            :void, [:pointer, :pointer, :pointer]
          ) do |client_ptr, event_ptr, opaque_ptr|
            BackgroundEventCallback.call(client_ptr, event_ptr, opaque_ptr)
          end

          # FFI Function used for Message Delivery callbacks
          delivery_callback_function = FFI::Function.new(
            :void, [:pointer, :pointer, :pointer]
          ) do |client_ptr, message_ptr, opaque_ptr|
            DeliveryCallback.call(client_ptr, message_ptr, opaque_ptr)
          end

          const_set(:BackgroundEventCallbackFunction, background_event_callback_function)
          const_set(:DeliveryCallbackFunction, delivery_callback_function)

          @@current_pid = ::Process.pid
        end
      end
    end
  end
end

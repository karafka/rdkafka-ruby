# frozen_string_literal: true

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

    class GroupResult
      attr_reader :result_error, :error_string, :result_name
      def initialize(group_result_pointer)
        native_error = Rdkafka::Bindings.rd_kafka_group_result_error(group_result_pointer)

        if native_error.null?
          @result_error = 0
          @error_string = FFI::Pointer::NULL
        else
          @result_error = native_error[:code]
          @error_string = native_error[:errstr]
        end

        @result_name = Rdkafka::Bindings.rd_kafka_group_result_name(group_result_pointer)
      end
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

      def initialize(acl_result_pointer)
        rd_kafka_error_pointer = Bindings.rd_kafka_acl_result_error(acl_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_error_code(rd_kafka_error_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_error_string(rd_kafka_error_pointer)
      end

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

      def initialize(acl_result_pointer)
        @matching_acls=[]
        rd_kafka_error_pointer = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_response_error(acl_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_error_code(rd_kafka_error_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_error_string(rd_kafka_error_pointer)
        if @result_error == 0
           # Get the number of matching acls
           pointer_to_size_t = FFI::MemoryPointer.new(:int32)
           @matching_acls = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_response_matching_acls(acl_result_pointer, pointer_to_size_t)
           @matching_acls_count = pointer_to_size_t.read_int
        end
      end

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

      def initialize(event_ptr)
        @matching_acls=[]
        @result_error = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)
        @error_string = Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr)
        if @result_error == 0
          acl_describe_result = Rdkafka::Bindings.rd_kafka_event_DescribeAcls_result(event_ptr)
          # Get the number of matching acls
          pointer_to_size_t = FFI::MemoryPointer.new(:int32)
          @matching_acls = Rdkafka::Bindings.rd_kafka_DescribeAcls_result_acls(acl_describe_result, pointer_to_size_t)
          @matching_acls_count = pointer_to_size_t.read_int
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
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT
          process_create_partitions(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_CREATEACLS_RESULT
          process_create_acl(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEACLS_RESULT
          process_delete_acl(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT
          process_describe_acl(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEGROUPS_RESULT
          process_delete_groups(event_ptr)
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

      def self.process_delete_groups(event_ptr)
        delete_groups_result = Rdkafka::Bindings.rd_kafka_event_DeleteGroups_result(event_ptr)

        # Get the number of delete group results
        pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
        delete_group_result_array = Rdkafka::Bindings.rd_kafka_DeleteGroups_result_groups(delete_groups_result, pointer_to_size_t)
        delete_group_results = GroupResult.create_group_results_from_array(pointer_to_size_t.read_int, delete_group_result_array) # TODO fix this
        delete_group_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if (delete_group_handle = Rdkafka::Admin::DeleteGroupsHandle.remove(delete_group_handle_ptr.address))
          delete_group_handle[:response] = delete_group_results[0].result_error
          delete_group_handle[:error_string] = delete_group_results[0].error_string
          delete_group_handle[:result_name] = delete_group_results[0].result_name
          delete_group_handle[:pending] = false
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

      def self.process_create_partitions(event_ptr)
        create_partitionss_result = Rdkafka::Bindings.rd_kafka_event_CreatePartitions_result(event_ptr)

        # Get the number of create topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_partitions_result_array = Rdkafka::Bindings.rd_kafka_CreatePartitions_result_topics(create_partitionss_result, pointer_to_size_t)
        create_partitions_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, create_partitions_result_array)
        create_partitions_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_partitions_handle = Rdkafka::Admin::CreatePartitionsHandle.remove(create_partitions_handle_ptr.address)
          create_partitions_handle[:response] = create_partitions_results[0].result_error
          create_partitions_handle[:error_string] = create_partitions_results[0].error_string
          create_partitions_handle[:result_name] = create_partitions_results[0].result_name
          create_partitions_handle[:pending] = false
        end
      end

      def self.process_create_acl(event_ptr)
        create_acls_result = Rdkafka::Bindings.rd_kafka_event_CreateAcls_result(event_ptr)

        # Get the number of acl results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_acl_result_array = Rdkafka::Bindings.rd_kafka_CreateAcls_result_acls(create_acls_result, pointer_to_size_t)
        create_acl_results = CreateAclResult.create_acl_results_from_array(pointer_to_size_t.read_int, create_acl_result_array)
        create_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_acl_handle = Rdkafka::Admin::CreateAclHandle.remove(create_acl_handle_ptr.address)
          create_acl_handle[:response] = create_acl_results[0].result_error
          create_acl_handle[:response_string] = create_acl_results[0].error_string
          create_acl_handle[:pending] = false
        end
      end

      def self.process_delete_acl(event_ptr)
        delete_acls_result = Rdkafka::Bindings.rd_kafka_event_DeleteAcls_result(event_ptr)

        # Get the number of acl results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        delete_acl_result_responses = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_responses(delete_acls_result, pointer_to_size_t)
        delete_acl_results = DeleteAclResult.delete_acl_results_from_array(pointer_to_size_t.read_int, delete_acl_result_responses)
        delete_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if delete_acl_handle = Rdkafka::Admin::DeleteAclHandle.remove(delete_acl_handle_ptr.address)
          delete_acl_handle[:response] = delete_acl_results[0].result_error
          delete_acl_handle[:response_string] = delete_acl_results[0].error_string
          delete_acl_handle[:pending] = false
          if delete_acl_results[0].result_error == 0
            delete_acl_handle[:matching_acls] = delete_acl_results[0].matching_acls
            delete_acl_handle[:matching_acls_count] = delete_acl_results[0].matching_acls_count
          end
        end
      end

      def self.process_describe_acl(event_ptr)
        describe_acl = DescribeAclResult.new(event_ptr)
        describe_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if describe_acl_handle = Rdkafka::Admin::DescribeAclHandle.remove(describe_acl_handle_ptr.address)
          describe_acl_handle[:response] = describe_acl.result_error
          describe_acl_handle[:response_string] = describe_acl.error_string
          describe_acl_handle[:pending] = false
          if describe_acl.result_error == 0
            describe_acl_handle[:acls]       = describe_acl.matching_acls
            describe_acl_handle[:acls_count] = describe_acl.matching_acls_count
          end
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
          topic_name = Rdkafka::Bindings.rd_kafka_topic_name(message[:rkt])

          # Update delivery handle
          delivery_handle[:response] = message[:err]
          delivery_handle[:partition] = message[:partition]
          delivery_handle[:offset] = message[:offset]
          delivery_handle[:topic_name] = FFI::MemoryPointer.from_string(topic_name)
          delivery_handle[:pending] = false

          # Call delivery callback on opaque
          if opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
            opaque.call_delivery_callback(Rdkafka::Producer::DeliveryReport.new(message[:partition], message[:offset], topic_name, message[:err]), delivery_handle)
          end
        end
      end
    end

  end
end

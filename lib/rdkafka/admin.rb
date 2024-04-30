# frozen_string_literal: true

module Rdkafka
  class Admin
    include Helpers::OAuth

    class << self
      # Allows us to retrieve librdkafka errors with descriptions
      # Useful for debugging and building UIs, etc.
      #
      # @return [Hash<Integer, Hash>] hash with errors mapped by code
      def describe_errors
        # Memory pointers for the array of structures and count
        p_error_descs = FFI::MemoryPointer.new(:pointer)
        p_count = FFI::MemoryPointer.new(:size_t)

        # Call the attached function
        Bindings.rd_kafka_get_err_descs(p_error_descs, p_count)

        # Retrieve the number of items in the array
        count = p_count.read_uint

        # Get the pointer to the array of error descriptions
        array_of_errors = FFI::Pointer.new(Bindings::NativeErrorDesc, p_error_descs.read_pointer)

        errors = {}

        count.times do |i|
          # Get the pointer to each struct
          error_ptr = array_of_errors[i]

          # Create a new instance of NativeErrorDesc for each item
          error_desc = Bindings::NativeErrorDesc.new(error_ptr)

          # Read values from the struct
          code = error_desc[:code]

          name = ''
          desc = ''

          name = error_desc[:name].read_string unless error_desc[:name].null?
          desc = error_desc[:desc].read_string unless error_desc[:desc].null?

          errors[code] = { code: code, name: name, description: desc }
        end

        errors
      end
    end

    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka

      # Makes sure, that native kafka gets closed before it gets GCed by Ruby
      ObjectSpace.define_finalizer(self, native_kafka.finalizer)
    end

    # Starts the native Kafka polling thread and kicks off the init polling
    # @note Not needed to run unless explicit start was disabled
    def start
      @native_kafka.start
    end

    # @return [String] admin name
    def name
      @name ||= @native_kafka.with_inner do |inner|
        ::Rdkafka::Bindings.rd_kafka_name(inner)
      end
    end

    def finalizer
      ->(_) { close }
    end

    # Performs the metadata request using admin
    #
    # @param topic_name [String, nil] metadat about particular topic or all if nil
    # @param timeout_ms [Integer] metadata request timeout
    # @return [Metadata] requested metadata
    def metadata(topic_name = nil, timeout_ms = 2_000)
      closed_admin_check(__method__)

      @native_kafka.with_inner do |inner|
        Metadata.new(inner, topic_name, timeout_ms)
      end
    end

    # Close this admin instance
    def close
      return if closed?
      ObjectSpace.undefine_finalizer(self)
      @native_kafka.close
    end

    # Whether this admin has closed
    def closed?
      @native_kafka.closed?
    end

    # Create a topic with the given partition count and replication factor
    #
    # @return [CreateTopicHandle] Create topic handle that can be used to wait for the result of
    #   creating the topic
    #
    # @raise [ConfigError] When the partition count or replication factor are out of valid range
    # @raise [RdkafkaError] When the topic name is invalid or the topic already exists
    # @raise [RdkafkaError] When the topic configuration is invalid
    def create_topic(topic_name, partition_count, replication_factor, topic_config={})
      closed_admin_check(__method__)

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
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      create_topic_handle = CreateTopicHandle.new
      create_topic_handle[:pending] = true
      create_topic_handle[:response] = -1
      CreateTopicHandle.register(create_topic_handle)
      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATETOPICS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_topic_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_CreateTopics(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        CreateTopicHandle.remove(create_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
      end

      create_topic_handle
    end

    def delete_group(group_id)
      closed_admin_check(__method__)

      # Create a rd_kafka_DeleteGroup_t representing the new topic
      delete_groups_ptr = Rdkafka::Bindings.rd_kafka_DeleteGroup_new(
        FFI::MemoryPointer.from_string(group_id)
      )

      pointer_array = [delete_groups_ptr]
      groups_array_ptr = FFI::MemoryPointer.new(:pointer)
      groups_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      delete_groups_handle = DeleteGroupsHandle.new
      delete_groups_handle[:pending] = true
      delete_groups_handle[:response] = -1
      DeleteGroupsHandle.register(delete_groups_handle)
      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETETOPICS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_groups_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DeleteGroups(
            inner,
            groups_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DeleteGroupsHandle.remove(delete_groups_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_DeleteGroup_destroy(delete_groups_ptr)
      end

      delete_groups_handle
    end

    # Deletes the named topic
    #
    # @return [DeleteTopicHandle] Delete topic handle that can be used to wait for the result of
    #   deleting the topic
    # @raise [RdkafkaError] When the topic name is invalid or the topic does not exist
    def delete_topic(topic_name)
      closed_admin_check(__method__)

      # Create a rd_kafka_DeleteTopic_t representing the topic to be deleted
      delete_topic_ptr = Rdkafka::Bindings.rd_kafka_DeleteTopic_new(FFI::MemoryPointer.from_string(topic_name))

      # Note that rd_kafka_DeleteTopics can create more than one topic at a time
      pointer_array = [delete_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      delete_topic_handle = DeleteTopicHandle.new
      delete_topic_handle[:pending] = true
      delete_topic_handle[:response] = -1
      DeleteTopicHandle.register(delete_topic_handle)
      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETETOPICS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_topic_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DeleteTopics(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DeleteTopicHandle.remove(delete_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
      end

      delete_topic_handle
    end

    # Creates extra partitions for a given topic
    #
    # @param topic_name [String]
    # @param partition_count [Integer] how many partitions we want to end up with for given topic
    #
    # @raise [ConfigError] When the partition count or replication factor are out of valid range
    # @raise [RdkafkaError] When the topic name is invalid or the topic already exists
    # @raise [RdkafkaError] When the topic configuration is invalid
    #
    # @return [CreateTopicHandle] Create topic handle that can be used to wait for the result of creating the topic
    def create_partitions(topic_name, partition_count)
      closed_admin_check(__method__)

      @native_kafka.with_inner do |inner|
        error_buffer = FFI::MemoryPointer.from_string(" " * 256)
        new_partitions_ptr = Rdkafka::Bindings.rd_kafka_NewPartitions_new(
          FFI::MemoryPointer.from_string(topic_name),
          partition_count,
          error_buffer,
          256
        )
        if new_partitions_ptr.null?
          raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
        end

        pointer_array = [new_partitions_ptr]
        topics_array_ptr = FFI::MemoryPointer.new(:pointer)
        topics_array_ptr.write_array_of_pointer(pointer_array)

        # Get a pointer to the queue that our request will be enqueued on
        queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
        if queue_ptr.null?
          Rdkafka::Bindings.rd_kafka_NewPartitions_destroy(new_partitions_ptr)
          raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
        end

        # Create and register the handle we will return to the caller
        create_partitions_handle = CreatePartitionsHandle.new
        create_partitions_handle[:pending] = true
        create_partitions_handle[:response] = -1
        CreatePartitionsHandle.register(create_partitions_handle)
        admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEPARTITIONS)
        Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_partitions_handle.to_ptr)

        begin
          Rdkafka::Bindings.rd_kafka_CreatePartitions(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        rescue Exception
          CreatePartitionsHandle.remove(create_partitions_handle.to_ptr.address)
          raise
        ensure
          Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
          Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
          Rdkafka::Bindings.rd_kafka_NewPartitions_destroy(new_partitions_ptr)
        end

        create_partitions_handle
      end
    end

    # Create acl
    # @param resource_type - values of type rd_kafka_ResourceType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7307
    #        valid values are:
    #           RD_KAFKA_RESOURCE_TOPIC   = 2
    #           RD_KAFKA_RESOURCE_GROUP   = 3
    #           RD_KAFKA_RESOURCE_BROKER  = 4
    # @param resource_pattern_type - values of type rd_kafka_ResourcePatternType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7320
    #        valid values are:
    #           RD_KAFKA_RESOURCE_PATTERN_MATCH    = 2
    #           RD_KAFKA_RESOURCE_PATTERN_LITERAL  = 3
    #           RD_KAFKA_RESOURCE_PATTERN_PREFIXED = 4
    # @param operation - values of type rd_kafka_AclOperation_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8403
    #        valid values are:
    #           RD_KAFKA_ACL_OPERATION_ALL              = 2
    #           RD_KAFKA_ACL_OPERATION_READ             = 3
    #           RD_KAFKA_ACL_OPERATION_WRITE            = 4
    #           RD_KAFKA_ACL_OPERATION_CREATE           = 5
    #           RD_KAFKA_ACL_OPERATION_DELETE           = 6
    #           RD_KAFKA_ACL_OPERATION_ALTER            = 7
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE         = 8
    #           RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION   = 9
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS = 10
    #           RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS    = 11
    #           RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE = 12
    # @param permission_type - values of type rd_kafka_AclPermissionType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8435
    #        valid values are:
    #           RD_KAFKA_ACL_PERMISSION_TYPE_DENY  = 2
    #           RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW = 3
    #
    # @return [CreateAclHandle] Create acl handle that can be used to wait for the result of creating the acl
    #
    # @raise [RdkafkaError]
    def create_acl(resource_type:, resource_name:, resource_pattern_type:, principal:, host:, operation:, permission_type:)
      closed_admin_check(__method__)

      # Create a rd_kafka_AclBinding_t representing the new acl
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      new_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
        resource_type,
        FFI::MemoryPointer.from_string(resource_name),
        resource_pattern_type,
        FFI::MemoryPointer.from_string(principal),
        FFI::MemoryPointer.from_string(host),
        operation,
        permission_type,
        error_buffer,
        256
      )
      if new_acl_ptr.null?
        raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
      end

      # Note that rd_kafka_CreateAcls can create more than one acl at a time
      pointer_array = [new_acl_ptr]
      acls_array_ptr = FFI::MemoryPointer.new(:pointer)
      acls_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end

      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(new_acl_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle that we will return to the caller
      create_acl_handle = CreateAclHandle.new
      create_acl_handle[:pending] = true
      create_acl_handle[:response] = -1
      CreateAclHandle.register(create_acl_handle)

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEACLS)
      end

      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_acl_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_CreateAcls(
            inner,
            acls_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        CreateAclHandle.remove(create_acl_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(new_acl_ptr)
      end

      create_acl_handle
    end

    # Delete acl
    #
    # @param resource_type - values of type rd_kafka_ResourceType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7307
    #        valid values are:
    #           RD_KAFKA_RESOURCE_TOPIC   = 2
    #           RD_KAFKA_RESOURCE_GROUP   = 3
    #           RD_KAFKA_RESOURCE_BROKER  = 4
    # @param resource_pattern_type - values of type rd_kafka_ResourcePatternType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7320
    #        valid values are:
    #           RD_KAFKA_RESOURCE_PATTERN_MATCH    = 2
    #           RD_KAFKA_RESOURCE_PATTERN_LITERAL  = 3
    #           RD_KAFKA_RESOURCE_PATTERN_PREFIXED = 4
    # @param operation - values of type rd_kafka_AclOperation_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8403
    #        valid values are:
    #           RD_KAFKA_ACL_OPERATION_ALL              = 2
    #           RD_KAFKA_ACL_OPERATION_READ             = 3
    #           RD_KAFKA_ACL_OPERATION_WRITE            = 4
    #           RD_KAFKA_ACL_OPERATION_CREATE           = 5
    #           RD_KAFKA_ACL_OPERATION_DELETE           = 6
    #           RD_KAFKA_ACL_OPERATION_ALTER            = 7
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE         = 8
    #           RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION   = 9
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS = 10
    #           RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS    = 11
    #           RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE = 12
    # @param permission_type - values of type rd_kafka_AclPermissionType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8435
    #        valid values are:
    #           RD_KAFKA_ACL_PERMISSION_TYPE_DENY  = 2
    #           RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW = 3
    # @return [DeleteAclHandle] Delete acl handle that can be used to wait for the result of deleting the acl
    #
    # @raise [RdkafkaError]
    def delete_acl(resource_type:, resource_name:, resource_pattern_type:, principal:, host:, operation:, permission_type:)
      closed_admin_check(__method__)

      # Create a rd_kafka_AclBinding_t representing the acl to be deleted
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)

      delete_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBindingFilter_new(
        resource_type,
        resource_name ? FFI::MemoryPointer.from_string(resource_name) : nil,
        resource_pattern_type,
        principal ? FFI::MemoryPointer.from_string(principal) : nil,
        host ? FFI::MemoryPointer.from_string(host) : nil,
        operation,
        permission_type,
        error_buffer,
        256
      )

      if delete_acl_ptr.null?
        raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
      end

      # Note that rd_kafka_DeleteAcls can delete more than one acl at a time
      pointer_array = [delete_acl_ptr]
      acls_array_ptr = FFI::MemoryPointer.new(:pointer)
      acls_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end

      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(new_acl_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle that we will return to the caller
      delete_acl_handle = DeleteAclHandle.new
      delete_acl_handle[:pending] = true
      delete_acl_handle[:response] = -1
      DeleteAclHandle.register(delete_acl_handle)

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETEACLS)
      end

      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_acl_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DeleteAcls(
            inner,
            acls_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DeleteAclHandle.remove(delete_acl_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(delete_acl_ptr)
      end

      delete_acl_handle
    end

    # Describe acls
    #
    # @param resource_type - values of type rd_kafka_ResourceType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7307
    #        valid values are:
    #           RD_KAFKA_RESOURCE_TOPIC   = 2
    #           RD_KAFKA_RESOURCE_GROUP   = 3
    #           RD_KAFKA_RESOURCE_BROKER  = 4
    # @param resource_pattern_type - values of type rd_kafka_ResourcePatternType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L7320
    #        valid values are:
    #           RD_KAFKA_RESOURCE_PATTERN_MATCH    = 2
    #           RD_KAFKA_RESOURCE_PATTERN_LITERAL  = 3
    #           RD_KAFKA_RESOURCE_PATTERN_PREFIXED = 4
    # @param operation - values of type rd_kafka_AclOperation_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8403
    #        valid values are:
    #           RD_KAFKA_ACL_OPERATION_ALL              = 2
    #           RD_KAFKA_ACL_OPERATION_READ             = 3
    #           RD_KAFKA_ACL_OPERATION_WRITE            = 4
    #           RD_KAFKA_ACL_OPERATION_CREATE           = 5
    #           RD_KAFKA_ACL_OPERATION_DELETE           = 6
    #           RD_KAFKA_ACL_OPERATION_ALTER            = 7
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE         = 8
    #           RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION   = 9
    #           RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS = 10
    #           RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS    = 11
    #           RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE = 12
    # @param permission_type - values of type rd_kafka_AclPermissionType_t
    #        https://github.com/confluentinc/librdkafka/blob/292d2a66b9921b783f08147807992e603c7af059/src/rdkafka.h#L8435
    #        valid values are:
    #           RD_KAFKA_ACL_PERMISSION_TYPE_DENY  = 2
    #           RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW = 3
    # @return [DescribeAclHandle] Describe acl handle that can be used to wait for the result of fetching acls
    #
    # @raise [RdkafkaError]
    def describe_acl(resource_type:, resource_name:, resource_pattern_type:, principal:, host:, operation:, permission_type:)
      closed_admin_check(__method__)

      # Create a rd_kafka_AclBinding_t with the filters to fetch existing acls
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      describe_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBindingFilter_new(
        resource_type,
        resource_name ? FFI::MemoryPointer.from_string(resource_name) : nil,
        resource_pattern_type,
        principal ? FFI::MemoryPointer.from_string(principal) : nil,
        host ? FFI::MemoryPointer.from_string(host) : nil,
        operation,
        permission_type,
        error_buffer,
        256
      )
      if describe_acl_ptr.null?
        raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
      end

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end

      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(new_acl_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle that we will return to the caller
      describe_acl_handle = DescribeAclHandle.new
      describe_acl_handle[:pending] = true
      describe_acl_handle[:response] = -1
      DescribeAclHandle.register(describe_acl_handle)

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DESCRIBEACLS)
      end

      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, describe_acl_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DescribeAcls(
            inner,
            describe_acl_ptr,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DescribeAclHandle.remove(describe_acl_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy(describe_acl_ptr)
      end

      describe_acl_handle
    end

    # Describe configs
    #
    # @param resources [Array<Hash>]  Array where elements are hashes with two keys:
    #   - `:resource_type` - numerical resource type based on Kafka API
    #   - `:resource_name` - string with resource name
    # @return [DescribeConfigsHandle] Describe config handle that can be used to wait for the
    #   result of fetching resources with their appropriate configs
    #
    # @raise [RdkafkaError]
    #
    # @note Several resources can be requested at one go, but only one broker at a time
    def describe_configs(resources)
      closed_admin_check(__method__)

      handle = DescribeConfigsHandle.new
      handle[:pending] = true
      handle[:response] = -1

      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end

      if queue_ptr.null?
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(
          inner,
          Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS
        )
      end

      DescribeConfigsHandle.register(handle)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, handle.to_ptr)

      pointer_array = resources.map do |resource_details|
        Rdkafka::Bindings.rd_kafka_ConfigResource_new(
          resource_details.fetch(:resource_type),
          FFI::MemoryPointer.from_string(
            resource_details.fetch(:resource_name)
          )
        )
      end

      configs_array_ptr = FFI::MemoryPointer.new(:pointer, pointer_array.size)
      configs_array_ptr.write_array_of_pointer(pointer_array)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DescribeConfigs(
            inner,
            configs_array_ptr,
            pointer_array.size,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DescribeConfigsHandle.remove(handle.to_ptr.address)

        raise
      ensure
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy_array(
          configs_array_ptr,
          pointer_array.size
        ) if configs_array_ptr
      end

      handle
    end

    # Alters in an incremental way all the configs provided for given resources
    #
    # @param resources_with_configs [Array<Hash>] resources with the configs key that contains
    # name, value and the proper op_type to perform on this value.
    #
    # @return [IncrementalAlterConfigsHandle] Incremental alter configs handle that can be used to
    #   wait for the result of altering resources with their appropriate configs
    #
    # @raise [RdkafkaError]
    #
    # @note Several resources can be requested at one go, but only one broker at a time
    # @note The results won't contain altered values but only the altered resources
    def incremental_alter_configs(resources_with_configs)
      closed_admin_check(__method__)

      handle = IncrementalAlterConfigsHandle.new
      handle[:pending] = true
      handle[:response] = -1

      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end

      if queue_ptr.null?
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(
          inner,
          Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS
        )
      end

      IncrementalAlterConfigsHandle.register(handle)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, handle.to_ptr)

      # Tu poprawnie tworzyc
      pointer_array = resources_with_configs.map do |resource_details|
        # First build the appropriate resource representation
        resource_ptr = Rdkafka::Bindings.rd_kafka_ConfigResource_new(
          resource_details.fetch(:resource_type),
          FFI::MemoryPointer.from_string(
            resource_details.fetch(:resource_name)
          )
        )

        resource_details.fetch(:configs).each do |config|
          Bindings.rd_kafka_ConfigResource_add_incremental_config(
            resource_ptr,
            config.fetch(:name),
            config.fetch(:op_type),
            config.fetch(:value)
          )
        end

        resource_ptr
      end

      configs_array_ptr = FFI::MemoryPointer.new(:pointer, pointer_array.size)
      configs_array_ptr.write_array_of_pointer(pointer_array)


      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_IncrementalAlterConfigs(
            inner,
            configs_array_ptr,
            pointer_array.size,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        IncrementalAlterConfigsHandle.remove(handle.to_ptr.address)

        raise
      ensure
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy_array(
          configs_array_ptr,
          pointer_array.size
        ) if configs_array_ptr
      end

      handle
    end

    private

    def closed_admin_check(method)
      raise Rdkafka::ClosedAdminError.new(method) if closed?
    end
  end
end

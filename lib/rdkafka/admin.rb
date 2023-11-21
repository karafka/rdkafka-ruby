# frozen_string_literal: true

module Rdkafka
  class Admin
    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka

      # Makes sure, that native kafka gets closed before it gets GCed by Ruby
      ObjectSpace.define_finalizer(self, native_kafka.finalizer)
    end

    def finalizer
      ->(_) { close }
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

    private
    def closed_admin_check(method)
      raise Rdkafka::ClosedAdminError.new(method) if closed?
    end
  end
end

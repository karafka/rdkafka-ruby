# frozen_string_literal: true

require "objspace"

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

    # Create an ACL with the given ACL options
    #
    # @raise [ConfigError] When the required acl_options are missing
    # @raise [ConfigError] When the ACL binding configuration is incorrect
    # @raise [RdkafkaError] When the ACL fails to be created
    #
    # @return [CreateACLHandle] Create ACL handle that can be used to wait for the result of creating the ACL
    def create_acl(name, principal, host, acl_options = {})
      closed_admin_check(__method__)

      %i[resource_type pattern_type acl_operation permission_type].each do |key|
        unless acl_options.key?(key)
          raise Rdkafka::Config::ConfigError,
                "Missing required key #{key} in acl_options hash"
        end
      end

      resource_type = acl_options[:resource_type]
      pattern_type = acl_options[:pattern_type]
      acl_operation = acl_options[:acl_operation]
      permission_type = acl_options[:permission_type]

      # Create a rd_kafka_AclBinding_new representing the new ACL
      error_buffer = FFI::MemoryPointer.from_string(' ' * 256)
      new_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
        resource_type,
        FFI::MemoryPointer.from_string(name),
        pattern_type,
        FFI::MemoryPointer.from_string(principal),
        FFI::MemoryPointer.from_string(host),
        acl_operation,
        permission_type,
        error_buffer,
        512
      )
      raise Rdkafka::Config::ConfigError, error_buffer.read_string if new_acl_ptr.null?

      # Note that rd_kafka_CreateAcls can create more than one ACL at a time
      pointer_array = [new_acl_ptr]
      acls_array_pointer = FFI::MemoryPointer.new(:pointer)
      acls_array_pointer.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(@native_kafka.inner)
      raise Rdkafka::Config::ConfigError.new('rd_kafka_queue_get_background was NULL') if queue_ptr.null?

      # Create and register the handle we will return to the caller
      create_acl_handle = CreateACLHandle.new
      create_acl_handle[:pending] = true
      create_acl_handle[:response] = -1
      CreateACLHandle.register(create_acl_handle)
      admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(@native_kafka.inner,
                                                                      Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEACLS)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_acl_handle.to_ptr)

      begin
        Rdkafka::Bindings.rd_kafka_CreateAcls(
          @native_kafka.inner,
          acls_array_pointer,
          1,
          admin_options_ptr,
          queue_ptr
        )
      rescue StandardError
        CreateACLHandle.remove(create_acl_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_AclBinding_destroy_array(acls_array_pointer, 1)
      end

      create_acl_handle
    end

    # Create a topic with the given partition count and replication factor
    #
    # @raise [ConfigError] When the partition count or replication factor are out of valid range
    # @raise [RdkafkaError] When the topic name is invalid or the topic already exists
    # @raise [RdkafkaError] When the topic configuration is invalid
    #
    # @return [CreateTopicHandle] Create topic handle that can be used to wait for the result of creating the topic
    def create_topic(topic_name, partition_count, replication_factor, topic_config = {})
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
      raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if new_topic_ptr.null?

      topic_config&.each do |key, value|
        Rdkafka::Bindings.rd_kafka_NewTopic_set_config(
          new_topic_ptr,
          key.to_s,
          value.to_s
        )
      end

      # Note that rd_kafka_CreateTopics can create more than one topic at a time
      pointer_array = [new_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(@native_kafka.inner)
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      create_topic_handle = CreateTopicHandle.new
      create_topic_handle[:pending] = true
      create_topic_handle[:response] = -1
      CreateTopicHandle.register(create_topic_handle)
      admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(@native_kafka.inner,
                                                                      Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATETOPICS)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_topic_handle.to_ptr)

      begin
        Rdkafka::Bindings.rd_kafka_CreateTopics(
          @native_kafka.inner,
          topics_array_ptr,
          1,
          admin_options_ptr,
          queue_ptr
        )
      rescue StandardError
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
      closed_admin_check(__method__)

      # Create a rd_kafka_DeleteTopic_t representing the topic to be deleted
      delete_topic_ptr = Rdkafka::Bindings.rd_kafka_DeleteTopic_new(FFI::MemoryPointer.from_string(topic_name))

      # Note that rd_kafka_DeleteTopics can create more than one topic at a time
      pointer_array = [delete_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = Rdkafka::Bindings.rd_kafka_queue_get_background(@native_kafka.inner)
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      delete_topic_handle = DeleteTopicHandle.new
      delete_topic_handle[:pending] = true
      delete_topic_handle[:response] = -1
      DeleteTopicHandle.register(delete_topic_handle)
      admin_options_ptr = Rdkafka::Bindings.rd_kafka_AdminOptions_new(@native_kafka.inner,
                                                                      Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETETOPICS)
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_topic_handle.to_ptr)

      begin
        Rdkafka::Bindings.rd_kafka_DeleteTopics(
          @native_kafka.inner,
          topics_array_ptr,
          1,
          admin_options_ptr,
          queue_ptr
        )
      rescue StandardError
        DeleteTopicHandle.remove(delete_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
      end

      delete_topic_handle
    end

    private

    def closed_admin_check(method)
      raise Rdkafka::ClosedAdminError.new(method) if closed?
    end
  end
end

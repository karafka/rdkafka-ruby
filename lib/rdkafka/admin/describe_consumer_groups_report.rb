# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for describe consumer groups operation result
    class DescribeConsumerGroupsReport
      # Described consumer groups, one per requested group id. Each entry is a hash with:
      #   - `:group_id` [String] the consumer group id
      #   - `:error` [RdkafkaError, nil] error describing this group, or `nil` on success. The
      #     other fields are not meaningful when it is set.
      #   - `:is_simple_consumer_group` [Boolean] `true` for a group that assigns partitions
      #     manually and uses Kafka only for offset storage
      #   - `:state` [Integer] the group state as a `Bindings::RD_KAFKA_CONSUMER_GROUP_STATE_*`
      #     code. A group id the cluster does not know is reported as
      #     `RD_KAFKA_CONSUMER_GROUP_STATE_DEAD` with no members.
      #   - `:state_name` [String] human-readable name of that state (e.g. `"Stable"`, `"Empty"`)
      #   - `:type` [Integer] the group protocol type as a
      #     `Bindings::RD_KAFKA_CONSUMER_GROUP_TYPE_*` code, one of:
      #     `RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC`,
      #     `RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER` (KIP-848),
      #     `RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN`
      #   - `:type_name` [String] human-readable name of that type (e.g. `"Classic"`, `"Consumer"`)
      #   - `:partition_assignor` [String, nil] partition assignor in use (e.g. `"range"`)
      #   - `:coordinator` [Hash, nil] the group coordinator broker as `{ id:, host:, port: }`
      #   - `:authorized_operations` [Array<Integer>, nil] ACL operations the client may perform on
      #     the group, as `Bindings::RD_KAFKA_ACL_OPERATION_*` codes. `nil` unless requested with
      #     `include_authorized_operations: true`.
      #   - `:members` [Array<Hash>] current group members, each with:
      #     - `:member_id` [String] the member (consumer) id
      #     - `:client_id` [String] the member's `client.id`
      #     - `:group_instance_id` [String, nil] the static membership instance id, if any
      #     - `:host` [String] the host the member connects from
      #     - `:assignment` [Hash{String => Array<Integer>}] assigned partitions, by topic
      #     - `:target_assignment` [Hash{String => Array<Integer>}, nil] partitions the member is
      #       moving to during a rebalance, by topic. Only `consumer` (KIP-848) groups have it;
      #       `nil` for `classic` groups.
      # @return [Array<Hash>]
      attr_reader :groups

      # @param result_ptr [FFI::Pointer] pointer to the `rd_kafka_DescribeConsumerGroups_result_t`
      def initialize(result_ptr)
        @groups = []

        return if result_ptr.null?

        count_ptr = FFI::MemoryPointer.new(:size_t)
        array_ptr = Bindings.rd_kafka_DescribeConsumerGroups_result_groups(result_ptr, count_ptr)

        return if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).each do |group_ptr|
          @groups << build_group(group_ptr)
        end
      end

      private

      # @param group_ptr [FFI::Pointer] pointer to the `rd_kafka_ConsumerGroupDescription_t`
      # @return [Hash]
      def build_group(group_ptr)
        state = Bindings.rd_kafka_ConsumerGroupDescription_state(group_ptr)
        type = Bindings.rd_kafka_ConsumerGroupDescription_type(group_ptr)

        {
          group_id: read_string(Bindings.rd_kafka_ConsumerGroupDescription_group_id(group_ptr)),
          error: build_error(Bindings.rd_kafka_ConsumerGroupDescription_error(group_ptr)),
          is_simple_consumer_group:
            Bindings.rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(group_ptr) != 0,
          state: state,
          state_name: read_string(Bindings.rd_kafka_consumer_group_state_name(state)),
          type: type,
          type_name: read_string(Bindings.rd_kafka_consumer_group_type_name(type)),
          partition_assignor:
            read_string(Bindings.rd_kafka_ConsumerGroupDescription_partition_assignor(group_ptr)),
          coordinator: build_node(Bindings.rd_kafka_ConsumerGroupDescription_coordinator(group_ptr)),
          authorized_operations: build_authorized_operations(group_ptr),
          members: Array.new(Bindings.rd_kafka_ConsumerGroupDescription_member_count(group_ptr)) do |index|
            build_member(Bindings.rd_kafka_ConsumerGroupDescription_member(group_ptr, index))
          end
        }
      end

      # @param member_ptr [FFI::Pointer] pointer to the `rd_kafka_MemberDescription_t`
      # @return [Hash]
      def build_member(member_ptr)
        {
          member_id: read_string(Bindings.rd_kafka_MemberDescription_consumer_id(member_ptr)),
          client_id: read_string(Bindings.rd_kafka_MemberDescription_client_id(member_ptr)),
          group_instance_id: read_string(Bindings.rd_kafka_MemberDescription_group_instance_id(member_ptr)),
          host: read_string(Bindings.rd_kafka_MemberDescription_host(member_ptr)),
          assignment: build_assignment(Bindings.rd_kafka_MemberDescription_assignment(member_ptr)),
          target_assignment: build_target_assignment(member_ptr)
        }
      end

      # @param assignment_ptr [FFI::Pointer] pointer to the `rd_kafka_MemberAssignment_t`
      # @return [Hash{String => Array<Integer>}]
      def build_assignment(assignment_ptr)
        return {} if assignment_ptr.null?

        tpl_ptr = Bindings.rd_kafka_MemberAssignment_partitions(assignment_ptr)

        return {} if tpl_ptr.null?

        Consumer::TopicPartitionList
          .from_native_tpl(tpl_ptr)
          .to_h
          .transform_values { |partitions| (partitions || []).map(&:partition) }
      end

      # @param member_ptr [FFI::Pointer] pointer to the `rd_kafka_MemberDescription_t`
      # @return [Hash{String => Array<Integer>}, nil]
      def build_target_assignment(member_ptr)
        assignment_ptr = Bindings.rd_kafka_MemberDescription_target_assignment(member_ptr)

        assignment_ptr.null? ? nil : build_assignment(assignment_ptr)
      end

      # @param group_ptr [FFI::Pointer] pointer to the `rd_kafka_ConsumerGroupDescription_t`
      # @return [Array<Integer>, nil]
      def build_authorized_operations(group_ptr)
        count_ptr = FFI::MemoryPointer.new(:size_t)
        operations_ptr = Bindings.rd_kafka_ConsumerGroupDescription_authorized_operations(group_ptr, count_ptr)

        return nil if operations_ptr.null?

        operations_ptr.read_array_of_int(count_ptr.read(:size_t))
      end

      # @param node_ptr [FFI::Pointer] pointer to the `rd_kafka_Node_t`
      # @return [Hash, nil]
      def build_node(node_ptr)
        return nil if node_ptr.null?

        {
          id: Bindings.rd_kafka_Node_id(node_ptr),
          host: read_string(Bindings.rd_kafka_Node_host(node_ptr)),
          port: Bindings.rd_kafka_Node_port(node_ptr)
        }
      end

      # @param error_ptr [FFI::Pointer] pointer to the `rd_kafka_error_t`
      # @return [RdkafkaError, nil]
      def build_error(error_ptr)
        return nil if error_ptr.null?

        RdkafkaError.new(
          Bindings.rd_kafka_error_code(error_ptr),
          broker_message: read_string(Bindings.rd_kafka_error_string(error_ptr))
        )
      end

      # @param ptr [FFI::Pointer] pointer to a C string
      # @return [String, nil]
      def read_string(ptr)
        ptr.null? ? nil : ptr.read_string
      end
    end
  end
end

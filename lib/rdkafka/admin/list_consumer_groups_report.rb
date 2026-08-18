# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for list consumer groups operation result
    class ListConsumerGroupsReport
      # Consumer groups listed cluster-wide. Each entry is a hash with:
      #   - `:group_id` [String] the consumer group id
      #   - `:is_simple_consumer_group` [Boolean] `true` for a "simple" consumer group - one that
      #     assigns partitions manually (via `assign`) and uses Kafka only for offset storage,
      #     rather than joining the group-management protocol and letting Kafka assign partitions
      #     and rebalance automatically (`subscribe`). Simple groups have no members from the
      #     broker's point of view, so they never rebalance.
      #   - `:state` [Integer] the group state as a `Bindings::RD_KAFKA_CONSUMER_GROUP_STATE_*`
      #     code, one of: `RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN`,
      #     `RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE`,
      #     `RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE`,
      #     `RD_KAFKA_CONSUMER_GROUP_STATE_STABLE`, `RD_KAFKA_CONSUMER_GROUP_STATE_DEAD`,
      #     `RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY`
      #   - `:state_name` [String] human-readable name of that state (e.g. `"Stable"`, `"Empty"`)
      # @return [Array<Hash>]
      attr_reader :groups

      # Per-broker errors reported alongside the (partial) group listing. `ListConsumerGroups`
      # fans out to every broker and returns valid groups and errors separately, so a broker
      # being unreachable does not discard the groups the reachable brokers returned.
      # @return [Array<RdkafkaError>]
      attr_reader :errors

      # @param result_ptr [FFI::Pointer] pointer to the `rd_kafka_ListConsumerGroups_result_t`
      def initialize(result_ptr)
        @groups = []
        @errors = []

        return if result_ptr.null?

        extract_groups(result_ptr)
        extract_errors(result_ptr)
      end

      private

      # @param result_ptr [FFI::Pointer] pointer to the result
      def extract_groups(result_ptr)
        count_ptr = FFI::MemoryPointer.new(:size_t)
        array_ptr = Bindings.rd_kafka_ListConsumerGroups_result_valid(result_ptr, count_ptr)

        return if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).each do |listing_ptr|
          state = Bindings.rd_kafka_ConsumerGroupListing_state(listing_ptr)
          group_id_ptr = Bindings.rd_kafka_ConsumerGroupListing_group_id(listing_ptr)
          state_name_ptr = Bindings.rd_kafka_consumer_group_state_name(state)

          @groups << {
            group_id: group_id_ptr.null? ? nil : group_id_ptr.read_string,
            is_simple_consumer_group:
              Bindings.rd_kafka_ConsumerGroupListing_is_simple_consumer_group(listing_ptr) != 0,
            state: state,
            state_name: state_name_ptr.null? ? nil : state_name_ptr.read_string
          }
        end
      end

      # @param result_ptr [FFI::Pointer] pointer to the result
      def extract_errors(result_ptr)
        count_ptr = FFI::MemoryPointer.new(:size_t)
        array_ptr = Bindings.rd_kafka_ListConsumerGroups_result_errors(result_ptr, count_ptr)

        return if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).each do |error_ptr|
          string_ptr = Bindings.rd_kafka_error_string(error_ptr)

          @errors << RdkafkaError.new(
            Bindings.rd_kafka_error_code(error_ptr),
            broker_message: string_ptr.null? ? nil : string_ptr.read_string
          )
        end
      end
    end
  end
end

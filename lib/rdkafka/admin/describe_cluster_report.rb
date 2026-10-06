# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for describe cluster operation result
    class DescribeClusterReport
      include DescribeResultParsing

      # @return [String, nil] the cluster id
      attr_reader :cluster_id

      # The current controller node, as a hash with `:id` [Integer], `:host` [String],
      # `:port` [Integer] and `:rack` [String, nil]
      # @return [Hash, nil]
      attr_reader :controller

      # Broker nodes of the cluster, each in the same shape as {#controller}
      # @return [Array<Hash>]
      attr_reader :nodes

      # Operations the client is authorized to perform on the cluster, as
      # `Bindings::RD_KAFKA_ACL_OPERATION_*` codes. `nil` unless requested with
      # `include_authorized_operations: true`.
      # @return [Array<Integer>, nil]
      attr_reader :authorized_operations

      # @param result_ptr [FFI::Pointer] pointer to the `rd_kafka_DescribeCluster_result_t`
      def initialize(result_ptr)
        @nodes = []

        return if result_ptr.null?

        cluster_id_ptr = Bindings.rd_kafka_DescribeCluster_result_cluster_id(result_ptr)
        @cluster_id = cluster_id_ptr.null? ? nil : cluster_id_ptr.read_string
        @controller = extract_node(Bindings.rd_kafka_DescribeCluster_result_controller(result_ptr))

        count_ptr = FFI::MemoryPointer.new(:size_t)
        @nodes = extract_nodes(Bindings.rd_kafka_DescribeCluster_result_nodes(result_ptr, count_ptr), count_ptr)

        count_ptr = FFI::MemoryPointer.new(:size_t)
        @authorized_operations = extract_authorized_operations(
          Bindings.rd_kafka_DescribeCluster_result_authorized_operations(result_ptr, count_ptr),
          count_ptr
        )
      end
    end
  end
end

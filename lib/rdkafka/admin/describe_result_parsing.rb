# frozen_string_literal: true

module Rdkafka
  class Admin
    # Helpers shared by the describe-topics and describe-cluster reports to copy nodes and
    # authorized operations out of event-owned memory.
    # @private
    module DescribeResultParsing
      private

      # @param node_ptr [FFI::Pointer] pointer to a `rd_kafka_Node_t`
      # @return [Hash, nil] node with `:id`, `:host`, `:port` and `:rack` or nil when NULL
      def extract_node(node_ptr)
        return nil if node_ptr.null?

        host_ptr = Bindings.rd_kafka_Node_host(node_ptr)
        rack_ptr = Bindings.rd_kafka_Node_rack(node_ptr)

        {
          id: Bindings.rd_kafka_Node_id(node_ptr),
          host: host_ptr.null? ? nil : host_ptr.read_string,
          port: Bindings.rd_kafka_Node_port(node_ptr),
          rack: rack_ptr.null? ? nil : rack_ptr.read_string
        }
      end

      # @param array_ptr [FFI::Pointer] pointer to a `rd_kafka_Node_t*` array
      # @param count_ptr [FFI::MemoryPointer] size_t holding the array length
      # @return [Array<Hash>] nodes as returned by {#extract_node}
      def extract_nodes(array_ptr, count_ptr)
        return [] if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).map { |node_ptr| extract_node(node_ptr) }
      end

      # @param array_ptr [FFI::Pointer] pointer to a `rd_kafka_AclOperation_t` array
      # @param count_ptr [FFI::MemoryPointer] size_t holding the array length
      # @return [Array<Integer>, nil] `Bindings::RD_KAFKA_ACL_OPERATION_*` codes or nil when the
      #   authorized operations were not requested
      def extract_authorized_operations(array_ptr, count_ptr)
        return nil if array_ptr.null?

        array_ptr.read_array_of_int(count_ptr.read(:size_t))
      end
    end
  end
end

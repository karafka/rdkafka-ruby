# frozen_string_literal: true

module Rdkafka
  class Admin
    class IncrementalAlterConfigsReport
      attr_reader :resources

      def initialize(config_entries:, entry_count:)
        @resources=[]

        return if config_entries == FFI::Pointer::NULL

        config_entries
          .read_array_of_pointer(entry_count)
          .each { |config_resource_result_ptr| validate!(config_resource_result_ptr) }
          .each do |config_resource_result_ptr|
            config_resource_result = ConfigResourceBindingResult.new(config_resource_result_ptr)

            pointer_to_size_t = FFI::MemoryPointer.new(:int32)
            configs_ptr = Bindings.rd_kafka_ConfigResource_configs(
              config_resource_result_ptr,
              pointer_to_size_t
            )

            configs_ptr
              .read_array_of_pointer(pointer_to_size_t.read_int)
              .map { |config_ptr| ConfigBindingResult.new(config_ptr) }
              .each { |config_binding| config_resource_result.configs << config_binding }

            @resources << config_resource_result
          end
      ensure
        return if config_entries == FFI::Pointer::NULL

        Bindings.rd_kafka_ConfigResource_destroy_array(config_entries, entry_count)
      end

      private

      def validate!(config_resource_result_ptr)
        RdkafkaError.validate!(
          Bindings.rd_kafka_ConfigResource_error(config_resource_result_ptr),
          Bindings.rd_kafka_ConfigResource_error_string(config_resource_result_ptr)
        )
      end
    end
  end
end

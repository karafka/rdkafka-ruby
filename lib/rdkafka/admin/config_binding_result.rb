# frozen_string_literal: true

module Rdkafka
  class Admin
    # A single config binding result that represents its values extracted from C
    class ConfigBindingResult
      attr_reader :name, :value, :read_only, :default, :sensitive, :synonym, :synonyms

      # @param config_ptr [FFI::Pointer] config pointer
      def initialize(config_ptr)
        @name = Bindings.rd_kafka_ConfigEntry_name(config_ptr)
        @value = Bindings.rd_kafka_ConfigEntry_value(config_ptr)
        @read_only = Bindings.rd_kafka_ConfigEntry_is_read_only(config_ptr)
        @default = Bindings.rd_kafka_ConfigEntry_is_default(config_ptr)
        @sensitive = Bindings.rd_kafka_ConfigEntry_is_sensitive(config_ptr)
        @synonym = Bindings.rd_kafka_ConfigEntry_is_synonym(config_ptr)
        @synonyms = []

        # The code below builds up the config synonyms using same config binding
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        synonym_ptr = Bindings.rd_kafka_ConfigEntry_synonyms(config_ptr, pointer_to_size_t)
        synonyms_ptr = synonym_ptr.read_array_of_pointer(pointer_to_size_t.read_int)

        (1..pointer_to_size_t.read_int).map do |ar|
          @synonyms << self.class.new(synonyms_ptr[ar - 1])
        end
      end
    end
  end
end

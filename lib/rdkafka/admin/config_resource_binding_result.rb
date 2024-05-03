# frozen_string_literal: true

module Rdkafka
  class Admin
    # A simple binding that represents the requested config resource
    class ConfigResourceBindingResult
      attr_reader :name, :type, :configs, :configs_count

      def initialize(config_resource_ptr)
        ffi_binding = Bindings::ConfigResource.new(config_resource_ptr)

        @name = ffi_binding[:name]
        @type = ffi_binding[:type]
        @configs = []
      end
    end
  end
end

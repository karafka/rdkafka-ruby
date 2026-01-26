# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for create ACL operation result
    class CreateAclReport
      # Upon successful creation of Acl RD_KAFKA_RESP_ERR_NO_ERROR - 0 is returned as rdkafka_response
      # @return [Integer]
      attr_reader :rdkafka_response

      # Upon successful creation of Acl empty string will be returned as rdkafka_response_string
      # @return [String]
      attr_reader :rdkafka_response_string

      # @param rdkafka_response [Integer] response code from librdkafka
      # @param rdkafka_response_string [FFI::Pointer] pointer to response string
      def initialize(rdkafka_response:, rdkafka_response_string:)
        @rdkafka_response = rdkafka_response
        if rdkafka_response_string != FFI::Pointer::NULL
          @rdkafka_response_string = rdkafka_response_string.read_string
        end
      end
    end
  end
end

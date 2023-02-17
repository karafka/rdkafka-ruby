# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateAclReport

      # Upon successful creation of Acl RD_KAFKA_RESP_ERR_NO_ERROR - 0 is returned as rdkafka_response
      # @return [Integer]
      attr_reader :rdkafka_response


      # Upon successful creation of Acl emtpy string will be returned as rdkafka_response_string
      # @return [String]
      attr_reader :rdkafka_response_string

      def initialize(rdkafka_response:, rdkafka_response_string:)
        @rdkafka_response = rdkafka_response
        if rdkafka_response_string != FFI::Pointer::NULL
          @rdkafka_response_string = rdkafka_response_string.read_string
        end
      end
    end
  end
end

module Rdkafka
  class Client
    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka
    end

    def oauthbearer_set_token(token, lifetime_ms, principal_name, extensions)
      raise ArgumentError, "token cannot be nil" if token.nil?
      raise ArgumentError, "principal_name cannot be nil" if principal_name.nil?

      extensions ||= []

      FFI::MemoryPointer.new(:char, 64) do |errstr_ptr|
        errstr_ptr.write_string("")

        FFI::MemoryPointer.new(:pointer, extensions.size) do |extensions_ptr|
          for i in 0..(extensions.size-1)
            extensions_ptr[i].write_pointer(FFI::MemoryPointer.from_string(extensions[i]))
          end

          ret = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
            @native_kafka,
            token, lifetime_ms, principal_name,
            extensions_ptr, extensions.size,
            errstr_ptr, 64)

          if ret == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
            return nil
          else
            return errstr_ptr.null? ? ret : errstr_ptr.read_string().force_encoding('UTF-8')
          end
        end
      end
    end

    def oauthbearer_set_token_failure(errstr)
      Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(@native_kafka, errstr)
    end
  end
end


require "ffi"
require "mini_portile2"

module Rdkafka
  module FFI
    extend ::FFI::Library
    ffi_lib "ext/ports/#{MiniPortile.new("librdkafka", Rdkafka::LIBRDKAFKA_VERSION).host}/librdkafka/#{Rdkafka::LIBRDKAFKA_VERSION}/lib/librdkafka.dylib"

    # Configuration

    enum :config_response, [
      :config_unknown, -2,
      :config_invalid, -1,
      :config_ok, 0
    ]

    attach_function :rd_kafka_conf_new, [], :pointer
    attach_function :rd_kafka_conf_set, [:pointer, :string, :string, :pointer, :int], :config_response
    attach_function :rd_kafka_conf_destroy, [:pointer], :void

  end
end

require "ffi"
require "mini_portile2"

module Rdkafka
  module Ffi
    extend FFI::Library
    ffi_lib "ext/ports/#{MiniPortile.new("librdkafka", Rdkafka::LIBRDKAFKA_VERSION).host}/librdkafka/#{Rdkafka::LIBRDKAFKA_VERSION}/lib/librdkafka.dylib"

    attach_function :rd_kafka_conf_new, [], :pointer

  end
end

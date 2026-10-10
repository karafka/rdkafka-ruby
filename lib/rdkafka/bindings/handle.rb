# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Handle

    enum :kafka_type, [
      :rd_kafka_producer,
      :rd_kafka_consumer
    ]

    attach_function :rd_kafka_new, [:kafka_type, :pointer, :pointer, :int], :pointer

    attach_function :rd_kafka_destroy, [:pointer], :void
  end
end

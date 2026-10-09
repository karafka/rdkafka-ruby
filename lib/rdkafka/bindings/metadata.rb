# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Metadata

    attach_function :rd_kafka_name, [:pointer], :string
    attach_function :rd_kafka_memberid, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_clusterid, [:pointer, :int], :pointer, blocking: true
    attach_function :rd_kafka_mem_free, [:pointer, :pointer], :void
    attach_function :rd_kafka_metadata, [:pointer, :int, :pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_metadata_destroy, [:pointer], :void, blocking: true
  end
end

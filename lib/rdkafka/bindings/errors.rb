# frozen_string_literal: true

module Rdkafka
  module Bindings
    attach_function :rd_kafka_err2name, [:int], :string
    attach_function :rd_kafka_err2str, [:int], :string
    attach_function :rd_kafka_error_is_fatal, [:pointer], :int
    attach_function :rd_kafka_error_is_retriable, [:pointer], :int
    attach_function :rd_kafka_error_txn_requires_abort, [:pointer], :int
    attach_function :rd_kafka_error_destroy, [:pointer], :void
    attach_function :rd_kafka_get_err_descs, [:pointer, :pointer], :void
  end
end

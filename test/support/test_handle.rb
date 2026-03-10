# frozen_string_literal: true

class TestHandle < Rdkafka::AbstractHandle
  layout :pending, :bool,
    :response, :int,
    :result, :int

  def operation_name
    "test_operation"
  end

  def create_result
    self[:result]
  end
end

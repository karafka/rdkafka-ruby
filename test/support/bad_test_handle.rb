# frozen_string_literal: true

class BadTestHandle < Rdkafka::AbstractHandle
  layout :pending, :bool,
    :response, :int
end

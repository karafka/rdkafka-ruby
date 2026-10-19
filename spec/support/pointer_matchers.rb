# frozen_string_literal: true

# Matches an FFI pointer by the address it points to. On JRuby an `FFI::MemoryPointer` and an
# `FFI::Pointer` to the same address are not `==`, so `with(pointer)` cannot match them.
#
# @example
#   expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(same_address_as(ptr))
RSpec::Matchers.define :same_address_as do |expected|
  match do |actual|
    actual.respond_to?(:address) && actual.address == expected.address
  end

  description { "a pointer to address #{expected.address}" }
end

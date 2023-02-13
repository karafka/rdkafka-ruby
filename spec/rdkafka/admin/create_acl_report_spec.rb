# frozen_string_literal: true

require 'spec_helper'

describe Rdkafka::Admin::CreateACLReport do
  subject do
    Rdkafka::Admin::CreateACLReport.new(
      FFI::MemoryPointer.from_string('error string'),
      1
    )
  end

  it 'should get the error string' do
    expect(subject.error_string).to eq('error string')
  end

  it 'should get the error code' do
    expect(subject.error_code).to eq(1)
  end
end

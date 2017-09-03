require "spec_helper"

describe Rdkafka::FFI do
  it "should load librdkafka" do
    expect(Rdkafka::FFI.ffi_libraries.map(&:name).first).to include "librdkafka"
  end

  it "should successfully call librdkafka" do
    expect {
      Rdkafka::FFI.rd_kafka_conf_new
    }.not_to raise_error
  end
end

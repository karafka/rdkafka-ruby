require "spec_helper"

describe Rdkafka::Bindings do
  it "should load librdkafka" do
    expect(Rdkafka::Bindings.ffi_libraries.map(&:name).first).to include "librdkafka"
  end

  it "should successfully call librdkafka" do
    expect {
      Rdkafka::Bindings.rd_kafka_conf_new
    }.not_to raise_error
  end
end

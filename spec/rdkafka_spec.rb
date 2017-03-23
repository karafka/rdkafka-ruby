require "spec_helper"

describe Rdkafka do
  it "should call a function in librdkafka" do
    Rdkafka::Ffi.rd_kafka_conf_new
  end
end

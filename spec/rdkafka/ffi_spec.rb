require "spec_helper"

describe Rdkafka::FFI do
  it "should successfully call librdkafka" do
    Rdkafka::FFI.rd_kafka_conf_new
  end
end

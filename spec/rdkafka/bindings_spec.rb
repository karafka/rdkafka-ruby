require "spec_helper"
require 'zlib'

describe Rdkafka::Bindings do
  it "should load librdkafka" do
    expect(Rdkafka::Bindings.ffi_libraries.map(&:name).first).to include "librdkafka"
  end

  describe ".lib_extension" do
    it "should know the lib extension for darwin" do
      stub_const('RbConfig::CONFIG', 'host_os' =>'darwin')
      expect(Rdkafka::Bindings.lib_extension).to eq "dylib"
    end

    it "should know the lib extension for linux" do
      stub_const('RbConfig::CONFIG', 'host_os' =>'linux')
      expect(Rdkafka::Bindings.lib_extension).to eq "so"
    end
  end

  it "should successfully call librdkafka" do
    expect {
      Rdkafka::Bindings.rd_kafka_conf_new
    }.not_to raise_error
  end

  describe "log callback" do
    let(:log_queue) { Rdkafka::Config.log_queue }
    before do
      allow(log_queue).to receive(:<<)
    end

    it "should log fatal messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::FATAL, "rdkafka: log line"])
    end

    it "should log error messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::ERROR, "rdkafka: log line"])
    end

    it "should log warning messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::WARN, "rdkafka: log line"])
    end

    it "should log info messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::INFO, "rdkafka: log line"])
    end

    it "should log debug messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::DEBUG, "rdkafka: log line"])
    end

    it "should log unknown messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 100, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::UNKNOWN, "rdkafka: log line"])
    end
  end

  describe "partitioner" do
    let(:partition_key) { ('a'..'z').to_a.shuffle.take(15).join('') }
    let(:partition_count) { rand(50) + 1 }

    it "should return the same partition for a similar string and the same partition count" do
      result_1 = Rdkafka::Bindings.partitioner(partition_key, partition_count)
      result_2 = Rdkafka::Bindings.partitioner(partition_key, partition_count)
      expect(result_1).to eq(result_2)
    end

    it "should match the old partitioner" do
      result_1 = Rdkafka::Bindings.partitioner(partition_key, partition_count)
      result_2 = (Zlib.crc32(partition_key) % partition_count)
      expect(result_1).to eq(result_2)
    end
  end

  describe "stats callback" do
    context "without a stats callback" do
      it "should do nothing" do
        expect {
          Rdkafka::Bindings::StatsCallback.call(nil, "{}", 2, nil)
        }.not_to raise_error
      end
    end

    context "with a stats callback" do
      before do
        Rdkafka::Config.statistics_callback = lambda do |stats|
          $received_stats = stats
        end
      end

      it "should call the stats callback with a stats hash" do
        Rdkafka::Bindings::StatsCallback.call(nil, "{\"received\":1}", 13, nil)
        expect($received_stats).to eq({'received' => 1})
      end
    end
  end

  describe "error callback" do
    context "without an error callback" do
      it "should do nothing" do
        expect {
          Rdkafka::Bindings::ErrorCallback.call(nil, 1, "error", nil)
        }.not_to raise_error
      end
    end

    context "with an error callback" do
      before do
        Rdkafka::Config.error_callback = lambda do |error|
          $received_error = error
        end
      end

      it "should call the error callback with an Rdkafka::Error" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)
        expect($received_error.code).to eq(:broker_not_available)
        expect($received_error.broker_message).to eq("Broker not available")
      end
    end
  end
end

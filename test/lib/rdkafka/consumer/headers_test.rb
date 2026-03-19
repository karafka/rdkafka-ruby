# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Consumer::Headers do
  before do
    @expected_headers = {
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }
  end

  describe ".from_native" do
    # Store the original FFI methods at class load time (before any test can remove them)
    ORIG_RD_KAFKA_MESSAGE_HEADERS = Rdkafka::Bindings.method(:rd_kafka_message_headers)
    ORIG_RD_KAFKA_HEADER_GET_ALL = Rdkafka::Bindings.method(:rd_kafka_header_get_all)

    before do
      @native_message = FFI::MemoryPointer.new(:int)
      @headers_ptr = FFI::MemoryPointer.new(:int)

      header_data = [
        { name: "version", value: "2.1.3" },
        { name: "version", value: "2.1.4" },
        { name: "type", value: "String" }
      ]

      headers_ptr = @headers_ptr
      bindings_meta = class << Rdkafka::Bindings; self; end

      # Remove existing singleton methods to avoid redefinition warnings
      bindings_meta.send(:remove_method, :rd_kafka_message_headers) if bindings_meta.method_defined?(:rd_kafka_message_headers, false)
      bindings_meta.send(:define_method, :rd_kafka_message_headers) do |msg, ptrptr|
        ptrptr.write_pointer(headers_ptr) if ptrptr.respond_to?(:write_pointer)
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      bindings_meta.send(:remove_method, :rd_kafka_header_get_all) if bindings_meta.method_defined?(:rd_kafka_header_get_all, false)
      bindings_meta.send(:define_method, :rd_kafka_header_get_all) do |ptr, idx, name_ptrptr, value_ptrptr, size_ptr|
        if idx < header_data.size
          hdr = header_data[idx]

          name_buf = FFI::MemoryPointer.from_string(hdr[:name])
          name_ptrptr.write_pointer(name_buf)

          value_buf = FFI::MemoryPointer.from_string(hdr[:value])
          value_ptrptr.write_pointer(value_buf)

          size_ptr[:value] = hdr[:value].bytesize

          Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
        else
          Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT
        end
      end
    end

    after do
      # Restore the original FFI-attached methods
      bindings_meta = class << Rdkafka::Bindings; self; end
      bindings_meta.send(:remove_method, :rd_kafka_message_headers) if bindings_meta.method_defined?(:rd_kafka_message_headers, false)
      bindings_meta.send(:define_method, :rd_kafka_message_headers, ORIG_RD_KAFKA_MESSAGE_HEADERS)
      bindings_meta.send(:remove_method, :rd_kafka_header_get_all) if bindings_meta.method_defined?(:rd_kafka_header_get_all, false)
      bindings_meta.send(:define_method, :rd_kafka_header_get_all, ORIG_RD_KAFKA_HEADER_GET_ALL)
    end

    def headers
      @headers ||= described_class.from_native(@native_message)
    end

    it "returns the expected headers" do
      assert_equal @expected_headers, headers
    end

    it "returns frozen headers" do
      assert headers.frozen?
    end

    it "returns array for duplicate headers" do
      assert_equal ["2.1.3", "2.1.4"], headers["version"]
    end

    it "returns string for single headers" do
      assert_equal "String", headers["type"]
    end

    it "does not support symbols mappings" do
      assert_equal false, headers.key?(:version)
    end
  end
end

# frozen_string_literal: true

describe Rdkafka::Consumer::Headers do
  let(:headers) do
    {
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }
  end

  def stub_header_calls(&block)
    headers_ptr = Object.new

    header_data = [
      { name: "version", value: headers["version"][0], size: headers["version"][0].size },
      { name: "version", value: headers["version"][1], size: headers["version"][1].size },
      { name: "type", value: headers["type"], size: headers["type"].size }
    ]

    native_message = Object.new

    message_headers_stub = proc do |msg, ptr|
      ptr.write_pointer(headers_ptr.object_id)
      Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
    end

    header_get_all_stub = proc do |_ptr, idx, name_ptrptr, value_ptrptr, size_ptr|
      if idx >= header_data.size
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT
      else
        data = header_data[idx]
        name_ptr = FFI::MemoryPointer.from_string(data[:name])
        name_ptrptr.write_pointer(name_ptr)

        value_ptr = FFI::MemoryPointer.from_string(data[:value])
        value_ptrptr.write_pointer(value_ptr)

        size_ptr[:value] = data[:size]

        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end
    end

    Rdkafka::Bindings.stub(:rd_kafka_message_headers, message_headers_stub) do
      Rdkafka::Bindings.stub(:rd_kafka_header_get_all, header_get_all_stub) do
        result = Rdkafka::Consumer::Headers.from_native(native_message)
        block.call(result)
      end
    end
  end

  describe ".from_native" do
    it "returns headers" do
      stub_header_calls do |result|
        assert_equal headers, result
      end
    end

    it "is frozen" do
      stub_header_calls do |result|
        assert_predicate result, :frozen?
      end
    end

    it "returns array for duplicate headers" do
      stub_header_calls do |result|
        assert_equal ["2.1.3", "2.1.4"], result["version"]
      end
    end

    it "returns string for single headers" do
      stub_header_calls do |result|
        assert_equal "String", result["type"]
      end
    end

    it "does not support symbol mappings" do
      stub_header_calls do |result|
        refute result.key?(:version)
      end
    end
  end
end

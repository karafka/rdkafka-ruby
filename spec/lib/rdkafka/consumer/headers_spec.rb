# frozen_string_literal: true

RSpec.describe Rdkafka::Consumer::Headers do
  let(:expected_headers) do
    { # Note String keys!
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }
  end
  let(:native_message) { double("native message") }
  let(:headers_ptr) { double("headers pointer") }

  describe ".from_native" do
    let(:headers) { described_class.from_native(native_message) }

    before do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_headers).with(native_message, anything) do |_, headers_ptrptr|
        expect(headers_ptrptr).to receive(:read_pointer).and_return(headers_ptr)
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      # First version header
      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
        .with(headers_ptr, 0, anything, anything, anything) do |_, _, name_ptrptr, value_ptrptr, size_ptr|
        expect(name_ptrptr).to receive(:read_pointer).and_return(double("pointer 0", read_string_to_null: "version"))
        expect(size_ptr).to receive(:[]).with(:value).and_return(expected_headers["version"][0].size)
        expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 0", read_string: expected_headers["version"][0]))
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      # Second version header
      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
        .with(headers_ptr, 1, anything, anything, anything) do |_, _, name_ptrptr, value_ptrptr, size_ptr|
        expect(name_ptrptr).to receive(:read_pointer).and_return(double("pointer 1", read_string_to_null: "version"))
        expect(size_ptr).to receive(:[]).with(:value).and_return(expected_headers["version"][1].size)
        expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 1", read_string: expected_headers["version"][1]))
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      # Single type header
      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
        .with(headers_ptr, 2, anything, anything, anything) do |_, _, name_ptrptr, value_ptrptr, size_ptr|
        expect(name_ptrptr).to receive(:read_pointer).and_return(double("pointer 2", read_string_to_null: "type"))
        expect(size_ptr).to receive(:[]).with(:value).and_return(expected_headers["type"].size)
        expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 2", read_string: expected_headers["type"]))
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
        .with(headers_ptr, 3, anything, anything, anything)
        .and_return(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
    end

    it { expect(headers).to eq(expected_headers) }
    it { expect(headers).to be_frozen }

    it "returns array for duplicate headers" do
      expect(headers["version"]).to eq(["2.1.3", "2.1.4"])
    end

    it "returns string for single headers" do
      expect(headers["type"]).to eq("String")
    end

    it "does not support symbols mappings" do
      expect(headers.key?(:version)).to be(false)
    end
  end

  describe ".from_native scratch pointers reuse" do
    let(:native_message) { double("native message") }

    before do
      allow(Rdkafka::Bindings)
        .to receive(:rd_kafka_message_headers)
        .and_return(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
    end

    it "does not allocate native scratch pointers after the first call on a thread" do
      # Warm up the per-thread scratch pointers
      described_class.from_native(native_message)

      expect(FFI::MemoryPointer).not_to receive(:new)
      expect(Rdkafka::Bindings::SizePtr).not_to receive(:new)

      expect(described_class.from_native(native_message)).to eq(described_class::EMPTY_HEADERS)
    end

    it "uses separate scratch pointers per thread" do
      pointers = Array.new(2) do
        Thread.new do
          described_class.from_native(native_message)
          Thread.current[:rdkafka_headers_scratch]
        end.value
      end

      expect(pointers[0]).not_to be_nil
      expect(pointers[0]).not_to eq(pointers[1])
    end

    it "uses separate scratch pointers per fiber within the same thread" do
      pointers = Array.new(2) do
        Fiber.new do
          described_class.from_native(native_message)
          Thread.current[:rdkafka_headers_scratch]
        end.resume
      end

      expect(pointers[0]).not_to be_nil
      expect(pointers[0]).not_to eq(pointers[1])
    end
  end
end

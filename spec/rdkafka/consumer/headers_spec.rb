# frozen_string_literal: true

describe Rdkafka::Consumer::Headers do
  let(:headers) do
    { # Note String keys!
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }
  end
  let(:native_message) { double('native message') }
  let(:headers_ptr) { double('headers pointer') }

  describe '.from_native' do
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
              expect(size_ptr).to receive(:[]).with(:value).and_return(headers["version"][0].size)
              expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 0", read_string: headers["version"][0]))
              Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
            end

      # Second version header
      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
          .with(headers_ptr, 1, anything, anything, anything) do |_, _, name_ptrptr, value_ptrptr, size_ptr|
              expect(name_ptrptr).to receive(:read_pointer).and_return(double("pointer 1", read_string_to_null: "version"))
              expect(size_ptr).to receive(:[]).with(:value).and_return(headers["version"][1].size)
              expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 1", read_string: headers["version"][1]))
              Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
            end

      # Single type header
      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
          .with(headers_ptr, 2, anything, anything, anything) do |_, _, name_ptrptr, value_ptrptr, size_ptr|
              expect(name_ptrptr).to receive(:read_pointer).and_return(double("pointer 2", read_string_to_null: "type"))
              expect(size_ptr).to receive(:[]).with(:value).and_return(headers["type"].size)
              expect(value_ptrptr).to receive(:read_pointer).and_return(double("value pointer 2", read_string: headers["type"]))
              Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
            end

      expect(Rdkafka::Bindings).to \
        receive(:rd_kafka_header_get_all)
          .with(headers_ptr, 3, anything, anything, anything)
          .and_return(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
    end

    subject { described_class.from_native(native_message) }

    it { is_expected.to eq(headers) }
    it { is_expected.to be_frozen }

    it 'returns array for duplicate headers' do
      expect(subject['version']).to eq(["2.1.3", "2.1.4"])
    end

    it 'returns string for single headers' do
      expect(subject['type']).to eq("String")
    end

    it 'does not support symbols mappings' do
      expect(subject.key?(:version)).to eq(false)
    end
  end
end

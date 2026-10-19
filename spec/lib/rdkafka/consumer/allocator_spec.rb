# frozen_string_literal: true

RSpec.describe Rdkafka::Consumer::Allocator do
  describe ".string_pointer" do
    subject(:pointer) { described_class.string_pointer(string) }

    after { Rdkafka::Bindings.rd_kafka_mem_free(nil, pointer) }

    let(:string) { "metadata" }

    it "returns a plain pointer that Ruby does not release" do
      expect(pointer).to be_an_instance_of(FFI::Pointer)
    end

    it "copies the string with a NUL terminator" do
      expect(pointer.get_bytes(0, string.bytesize + 1)).to eq("metadata\0")
      expect(pointer.read_string).to eq(string)
    end

    context "when the string is multibyte" do
      let(:string) { "zażółć" }

      it "copies all of its bytes" do
        expect(pointer.get_bytes(0, string.bytesize).force_encoding(Encoding::UTF_8)).to eq(string)
        expect(pointer.get_char(string.bytesize)).to eq(0)
      end
    end

    context "when the string is nil" do
      let(:string) { nil }

      it "returns an empty C string" do
        expect(pointer.get_char(0)).to eq(0)
      end
    end
  end
end

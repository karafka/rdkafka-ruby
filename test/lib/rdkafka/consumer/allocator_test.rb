# frozen_string_literal: true

describe Rdkafka::Consumer::Allocator do
  describe ".string_pointer" do
    before { @string = "metadata" }

    after { Rdkafka::Bindings.rd_kafka_mem_free(nil, @pointer) if @pointer }

    def build_pointer
      @pointer = described_class.string_pointer(@string)
    end

    it "returns a plain pointer that Ruby does not release" do
      assert_instance_of(FFI::Pointer, build_pointer)
    end

    it "copies the string with a NUL terminator" do
      pointer = build_pointer

      assert_equal("metadata\0", pointer.get_bytes(0, @string.bytesize + 1))
      assert_equal(@string, pointer.read_string)
    end

    context "when the string is multibyte" do
      before { @string = "zażółć" }

      it "copies all of its bytes" do
        pointer = build_pointer

        assert_equal(@string, pointer.get_bytes(0, @string.bytesize).force_encoding(Encoding::UTF_8))
        assert_equal(0, pointer.get_char(@string.bytesize))
      end
    end

    context "when the string is nil" do
      before { @string = nil }

      it "returns an empty C string" do
        assert_equal(0, build_pointer.get_char(0))
      end
    end
  end
end

# frozen_string_literal: true

describe Rdkafka::Consumer::Headers do
  before do
    @expected_headers = { # Note String keys!
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }
    @native_message = mock("native message")
    @headers_ptr = mock("headers pointer")
  end

  describe ".from_native" do
    before do
      # Start without cached scratch pointers so the mocked ones below get allocated and used
      Thread.current[:rdkafka_headers_scratch] = nil

      headers_ptrptr = mock("headers pointer pointer")
      name_ptrptr = mock("name pointer pointer")
      value_ptrptr = mock("value pointer pointer")
      size_ptr = mock("size pointer")

      FFI::MemoryPointer.stubs(:new).with(:pointer).returns(headers_ptrptr, name_ptrptr, value_ptrptr)
      Rdkafka::Bindings::SizePtr.stubs(:new).returns(size_ptr)

      Rdkafka::Bindings
        .expects(:rd_kafka_message_headers)
        .with(@native_message, headers_ptrptr)
        .returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)
      headers_ptrptr.expects(:read_pointer).returns(@headers_ptr)

      name_ptrptr.expects(:read_pointer).times(3).returns(
        # First version header
        mock("pointer 0", read_string_to_null: "version"),
        # Second version header
        mock("pointer 1", read_string_to_null: "version"),
        # Single type header
        mock("pointer 2", read_string_to_null: "type")
      )
      size_ptr.expects(:[]).with(:value).times(3).returns(
        @expected_headers["version"][0].size,
        @expected_headers["version"][1].size,
        @expected_headers["type"].size
      )
      value_ptrptr.expects(:read_pointer).times(3).returns(
        mock("value pointer 0", read_string: @expected_headers["version"][0]),
        mock("value pointer 1", read_string: @expected_headers["version"][1]),
        mock("value pointer 2", read_string: @expected_headers["type"])
      )

      3.times do |idx|
        Rdkafka::Bindings
          .expects(:rd_kafka_header_get_all)
          .with(@headers_ptr, idx, name_ptrptr, value_ptrptr, size_ptr)
          .returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)
      end

      Rdkafka::Bindings
        .expects(:rd_kafka_header_get_all)
        .with(@headers_ptr, 3, name_ptrptr, value_ptrptr, size_ptr)
        .returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)

      @headers = described_class.from_native(@native_message)
    end

    # Do not leak the mocked scratch pointers into other tests
    after { Thread.current[:rdkafka_headers_scratch] = nil }

    it "equals the expected headers" do
      assert_equal(@expected_headers, @headers)
    end

    it "is frozen" do
      assert_predicate(@headers, :frozen?)
    end

    it "returns array for duplicate headers" do
      assert_equal(["2.1.3", "2.1.4"], @headers["version"])
    end

    it "returns string for single headers" do
      assert_equal("String", @headers["type"])
    end

    it "does not support symbols mappings" do
      refute(@headers.key?(:version))
    end
  end

  describe ".from_native scratch pointers reuse" do
    before do
      @native_message = mock("native message")

      Rdkafka::Bindings
        .stubs(:rd_kafka_message_headers)
        .returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
    end

    it "does not allocate native scratch pointers after the first call on a thread" do
      # Warm up the per-thread scratch pointers
      described_class.from_native(@native_message)

      FFI::MemoryPointer.expects(:new).never
      Rdkafka::Bindings::SizePtr.expects(:new).never

      assert_equal(described_class::EMPTY_HEADERS, described_class.from_native(@native_message))
    end

    it "uses separate scratch pointers per thread" do
      pointers = Array.new(2) do
        Thread.new do
          described_class.from_native(@native_message)
          Thread.current[:rdkafka_headers_scratch]
        end.value
      end

      refute_nil(pointers[0])
      refute_equal(pointers[1], pointers[0])
    end

    it "uses separate scratch pointers per fiber within the same thread" do
      pointers = Array.new(2) do
        Fiber.new do
          described_class.from_native(@native_message)
          Thread.current[:rdkafka_headers_scratch]
        end.resume
      end

      refute_nil(pointers[0])
      refute_equal(pointers[1], pointers[0])
    end
  end
end

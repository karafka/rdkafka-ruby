# frozen_string_literal: true

describe Rdkafka::AbstractHandle do
  def build_handle(pending_handle, response: 0, result: -1)
    TestHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:result] = result
    end
  end

  context "A subclass that does not implement the required methods" do
    class BadTestHandle < Rdkafka::AbstractHandle
      layout :pending, :bool,
        :response, :int
    end

    it "raises an exception if operation_name is called" do
      error = assert_raises(RuntimeError) do
        BadTestHandle.new.operation_name
      end

      assert_match(/Must be implemented by subclass!/, error.message)
    end

    it "raises an exception if create_result is called" do
      error = assert_raises(RuntimeError) do
        BadTestHandle.new.create_result
      end

      assert_match(/Must be implemented by subclass!/, error.message)
    end
  end

  class TestHandle < Rdkafka::AbstractHandle
    layout :pending, :bool,
      :response, :int,
      :result, :int

    def operation_name
      "test_operation"
    end

    def create_result
      self[:result]
    end
  end

  describe ".register and .remove" do
    before { @handle = build_handle(true) }

    it "registers and remove a delivery handle" do
      described_class.register(@handle)
      removed = described_class.remove(@handle.to_ptr.address)

      assert_equal(@handle, removed)
      assert_empty(Rdkafka::AbstractHandle::REGISTRY)
    end

    it "registers and removes handles consistently under concurrent access" do
      # The registry is mutated from producing/consuming threads and the background polling thread
      # (which removes handles from FFI callbacks). Hammer it from many threads and assert no
      # registration or removal is lost. On MRI the GVL hides the race; this guards JRuby, where the
      # plain Hash mutations are not atomic.
      handles = Array.new(200) { TestHandle.new }

      handles.map do |h|
        Thread.new do
          described_class.register(h)
          described_class.remove(h.to_ptr.address)
        end
      end.each(&:join)

      handles.each do |h|
        refute_includes(Rdkafka::AbstractHandle::REGISTRY, h.to_ptr.address)
      end

      assert_empty(Rdkafka::AbstractHandle::REGISTRY)
    end
  end

  describe ".registered?" do
    before { @handle = build_handle(true) }

    it "is true only while the handle is registered" do
      refute(described_class.registered?(@handle))

      described_class.register(@handle)

      assert(described_class.registered?(@handle))

      described_class.remove(@handle.to_ptr.address)

      refute(described_class.registered?(@handle))
    end

    it "is false for another handle registered at the same address" do
      other = TestHandle.new(@handle.to_ptr)
      described_class.register(other)

      refute(described_class.registered?(@handle))
    ensure
      described_class.remove(@handle.to_ptr.address)
    end
  end

  describe "#pending?" do
    context "when true" do
      it "is true" do
        assert_predicate(build_handle(true), :pending?)
      end
    end

    context "when not true" do
      it "is false" do
        refute_predicate(build_handle(false), :pending?)
      end
    end
  end

  describe "#wait" do
    context "when pending_handle true" do
      it "waits until the timeout and then raise an error" do
        handle = build_handle(true)

        error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
          handle.wait(max_wait_timeout_ms: 100)
        end

        assert_match(/test_operation/, error.message)
      end
    end

    context "when pending_handle false" do
      context "without error" do
        before { @handle = build_handle(false, result: 1) }

        it "returns a result" do
          wait_result = @handle.wait

          assert_equal(1, wait_result)
        end

        it "waits without a timeout" do
          wait_result = @handle.wait(max_wait_timeout_ms: nil)

          assert_equal(1, wait_result)
        end
      end

      context "with error" do
        it "raises an rdkafka error" do
          handle = build_handle(false, response: 20)

          assert_raises(Rdkafka::RdkafkaError) do
            handle.wait
          end
        end
      end

      context "backwards compatibility with max_wait_timeout (seconds)" do
        before { @handle = build_handle(false, result: 42) }

        it "works with max_wait_timeout (emits deprecation warning to stderr)" do
          # Note: Deprecation warning is emitted but not tested here due to stderr capture complexity
          wait_result = @handle.wait(max_wait_timeout: 5)

          assert_equal(42, wait_result)
        end

        it "works with max_wait_timeout set to nil (wait forever)" do
          wait_result = @handle.wait(max_wait_timeout: nil)

          assert_equal(42, wait_result)
        end

        it "properlies convert seconds to milliseconds" do
          # Using a very short timeout to verify conversion
          @handle[:pending] = true

          error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
            @handle.wait(max_wait_timeout: 0.1)
          end

          assert_match(/100 ms/, error.message)
        end

        it "uses new parameter when both are provided" do
          # When both parameters provided, max_wait_timeout_ms takes precedence
          wait_result = @handle.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)

          assert_equal(42, wait_result)
        end

        it "timeouts based on max_wait_timeout_ms when both are provided" do
          @handle[:pending] = true

          # max_wait_timeout: 10 would be 10000ms, but max_wait_timeout_ms: 100 should take precedence
          error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
            @handle.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
          end

          assert_match(/100 ms/, error.message)
        end
      end
    end
  end

  describe "#unlock" do
    before { @handle = build_handle(true) }

    def start_waiter(handle)
      waiter = Thread.new { handle.wait(max_wait_timeout_ms: 10_000) }
      deadline = monotonic_now + 5
      sleep(0.001) until waiter.status == "sleep" || monotonic_now > deadline
      waiter
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # More pending handles than lock stripes, so at least two of them share one
    it "ends only the wait of the unlocked handle when handles share a stripe" do
      handles = Array.new(1_025) do |index|
        TestHandle.new.tap do |test_handle|
          test_handle[:pending] = true
          test_handle[:result] = index
        end
      end

      waiters = handles.map do |test_handle|
        Thread.new do
          value = test_handle.wait(max_wait_timeout_ms: 60_000)
          [value, test_handle.pending?]
        end
      end

      deadline = monotonic_now + 30
      sleep(0.01) until waiters.all? { |waiter| waiter.status == "sleep" } || monotonic_now > deadline

      handles.zip(waiters).each_with_index do |(test_handle, waiter), index|
        test_handle.unlock

        assert_equal([index, false], waiter.join(5)&.value)
      end
    ensure
      waiters&.each(&:kill)
    end

    it "wakes every thread waiting on the same handle" do
      @handle[:result] = 7
      waiters = Array.new(8) { start_waiter(@handle) }

      @handle.unlock

      assert_equal(Array.new(8, 7), waiters.map { |waiter| waiter.join(5)&.value })
    end
  end
end

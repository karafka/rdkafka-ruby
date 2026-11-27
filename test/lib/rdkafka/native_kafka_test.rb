# frozen_string_literal: true

describe Rdkafka::NativeKafka do
  before do
    @opaque = Rdkafka::Opaque.new
    @thread = mock("thread")

    Rdkafka::Bindings.stubs(:rd_kafka_name).returns("producer-1")
    Thread.stubs(:new).returns(@thread)
    @thread.stubs(:name=).with("rdkafka.native_kafka#producer-1")
    @thread.stubs(:[]=).with(:closing, anything)
    @thread.stubs(:join)
    @thread.stubs(:abort_on_exception=).with(anything)
  end

  after { client.close }

  def native
    @native ||= begin
      config = rdkafka_producer_config
      config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
    end
  end

  def client
    @client ||= described_class.new(native, run_polling_thread: true, opaque: @opaque)
  end

  context "defaults" do
    it "sets the thread name" do
      @thread.expects(:name=).with("rdkafka.native_kafka#producer-1")

      client
    end

    it "sets the thread to abort on exception" do
      @thread.expects(:abort_on_exception=).with(true)

      client
    end

    it "sets the thread `closing` flag to false" do
      @thread.expects(:[]=).with(:closing, false)

      client
    end
  end

  context "the polling thread" do
    it "is created" do
      Thread.expects(:new).returns(@thread)

      client
    end
  end

  it "exposes the inner client" do
    client.with_inner do |inner|
      assert_equal(native, inner)
    end
  end

  context "when client was not yet closed (`nil`)" do
    it "is not closed" do
      refute_predicate(client, :closed?)
    end

    context "and attempt to close" do
      it "calls the `destroy` binding" do
        real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
        client
        Rdkafka::Bindings.expects(:rd_kafka_destroy).with(native)

        client.close
      ensure
        real_destroy&.call(native) if client.closed?
      end

      it "indicates to the polling thread that it is closing" do
        @thread.expects(:[]=).with(:closing, true)

        client.close
      end

      it "joins the polling thread" do
        @thread.expects(:join)

        client.close
      end

      it "closes and unassign the native client" do
        client.close

        assert_predicate(client, :closed?)
      end

      context "when an interrupt is delivered as the `destroy` binding returns" do
        before do
          @real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
          client
          Rdkafka::Bindings.stubs(:rd_kafka_destroy).raises(RuntimeError, "interrupted")
        end

        # The stub cannot call the real binding first, so the native client is destroyed here
        after { @real_destroy.call(native) }

        it "does not hand out the destroyed native client" do
          error = assert_raises(RuntimeError) { client.close }

          assert_equal("interrupted", error.message)
          assert_predicate(client, :closed?)
          assert_raises(Rdkafka::ClosedInnerError) { client.with_inner { |inner| inner } }
        end
      end
    end
  end

  context "when client was already closed" do
    before { client.close }

    it "is closed" do
      assert_predicate(client, :closed?)
    end

    context "and attempt to close again" do
      it "does not call the `destroy` binding" do
        Rdkafka::Bindings.expects(:rd_kafka_destroy_flags).never

        client.close
      end

      it "does not indicate to the polling thread that it is closing" do
        @thread.expects(:[]=).with(:closing, true).never

        client.close
      end

      it "does not join the polling thread" do
        @thread.expects(:join).never

        client.close
      end

      it "does not close and unassign the native client again" do
        client.close

        assert_predicate(client, :closed?)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    refute_predicate(client, :closed?)

    client.finalizer.call("some-ignored-object-id")

    assert_predicate(client, :closed?)
  end
end

# Separate describe block for FD API tests to avoid interference with mocked threading tests
describe Rdkafka::NativeKafka, "#enable_main_queue_io_events and #enable_background_queue_io_events" do
  before do
    config = rdkafka_producer_config
    native = config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
    @client = described_class.new(native, run_polling_thread: false, opaque: Rdkafka::Opaque.new, auto_start: false)
  end

  after { @client.close unless @client.closed? }

  it "allows IO events when polling thread is not active" do
    signal_r, signal_w = IO.pipe

    @client.enable_main_queue_io_events(signal_w.fileno)
    @client.enable_background_queue_io_events(signal_w.fileno)

    signal_r.close
    signal_w.close
  end

  it "accepts custom payload for IO events" do
    signal_r, signal_w = IO.pipe
    payload = "custom"

    @client.enable_main_queue_io_events(signal_w.fileno, payload)

    signal_r.close
    signal_w.close
  end

  context "when client is closed" do
    before { @client.close }

    it "raises ClosedInnerError when enabling main_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) { @client.enable_main_queue_io_events(signal_w.fileno) }
      signal_r.close
      signal_w.close
    end

    it "raises ClosedInnerError when enabling background_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) { @client.enable_background_queue_io_events(signal_w.fileno) }
      signal_r.close
      signal_w.close
    end
  end
end

# Separate describe block with real threads, which the mocked threading tests above stub out
describe Rdkafka::NativeKafka, "#with_inner from many threads" do
  context "when many threads use a producer" do
    before do
      @producer = rdkafka_producer_config.producer
      @threads_count = 16
    end

    after { @producer.close unless @producer.closed? }

    it "closes once the threads are done" do
      barrier = Queue.new
      stop = false

      threads = Array.new(@threads_count) do
        Thread.new do
          barrier.pop
          @producer.queue_size until stop
        end
      end

      @threads_count.times { barrier << true }
      sleep(2)
      stop = true
      threads.each(&:join)

      closer = Thread.new { @producer.close }

      refute_nil(closer.join(5))
    end
  end

  context "when operations overlap so that one is always in progress" do
    before { @consumer = rdkafka_consumer_config.consumer }

    after { @consumer.close unless @consumer.closed? }

    it "does not start new operations while close waits" do
      threads = Array.new(2) do |i|
        Thread.new do
          sleep(0.025 * i)

          loop { @consumer.poll(50) }
        rescue Rdkafka::ClosedConsumerError, Rdkafka::ClosedInnerError
          nil
        end
      end

      sleep(0.2)
      closer = Thread.new { @consumer.close }

      refute_nil(closer.join(5))
      threads.each(&:join)
    end
  end
end

# Separate describe block with real threads parked by TracePoint, to deliver an interrupt at an
# exact point of `#with_inner`
describe Rdkafka::NativeKafka, "#with_inner when the calling thread is interrupted" do
  before do
    skip "TracePoint core method events need --debug on JRuby" if defined?(JRUBY_VERSION)

    @producer = rdkafka_producer_config.producer
    @release = Queue.new
    @parked = Queue.new

    @producer.queue_size
  end

  after { @producer.close unless @producer.nil? || @producer.closed? }

  # Runs `producer.queue_size` in a new thread and parks it when the given `Mutex#synchronize`
  # call that `#with_inner` makes starts (`:call`) or ends (`:return`): the first takes the lock
  # for the increment, the second the lock for the decrement. `Mutex#synchronize` is a C method
  # before Ruby 4.0, so both kinds of events are traced.
  def park_worker(event, call)
    park_worker_on(event, :"c_#{event}", call: call) do |tp|
      tp.defined_class == Thread::Mutex && tp.method_id == :synchronize
    end
  end

  # Runs `producer.queue_size` in a new thread and parks it at the given line of
  # `lib/rdkafka/native_kafka.rb`, found by its code
  def park_worker_at_line(code)
    path = Rdkafka::NativeKafka.instance_method(:with_inner).source_location.first
    lineno = File.readlines(path).index { |line| line.strip == code } + 1

    park_worker_on(:line, call: 1) { |tp| tp.path == path && tp.lineno == lineno }
  end

  def park_worker_on(*events, call:, &matches)
    calls = 0
    worker = nil

    trace = TracePoint.new(*events) do |tp|
      next unless Thread.current == worker
      next unless matches.call(tp)

      calls += 1
      next unless calls == call

      @parked << true
      @release.pop
    end

    worker = Thread.new do
      Thread.stop
      @producer.queue_size
    rescue
      nil
    end

    sleep(0.01) until worker.status == "sleep"
    trace.enable
    worker.run

    assert_same(true, @parked.pop(timeout: 5))

    [worker, trace]
  end

  def assert_close_finishes(worker, trace)
    @release << true
    worker.join(5)
    trace.disable

    closer = Thread.new { @producer.close }

    refute_nil(closer.join(5))
  ensure
    closer&.kill
  end

  context "when raised before it takes the lock for the increment" do
    it "lets close finish" do
      worker, trace = park_worker(:call, 1)
      worker.raise("interrupted")

      assert_close_finishes(worker, trace)
    end
  end

  context "when raised right after the increment" do
    it "lets close finish" do
      worker, trace = park_worker(:return, 1)
      worker.raise("interrupted")

      assert_close_finishes(worker, trace)
    end
  end

  context "when raised before it takes the lock for the decrement" do
    it "lets close finish" do
      worker, trace = park_worker(:call, 2)
      worker.raise("interrupted")

      assert_close_finishes(worker, trace)
    end
  end

  context "when raised at the first step of the release" do
    it "lets close finish" do
      worker, trace = park_worker_at_line("Thread.handle_interrupt(DEFER_INTERRUPTS) do")
      worker.raise("interrupted")

      assert_close_finishes(worker, trace)
    end
  end

  context "when killed before it takes the lock for the decrement" do
    it "lets close finish" do
      worker, trace = park_worker(:call, 2)
      worker.kill

      assert_close_finishes(worker, trace)
    end
  end
end

# Separate describe block: trap handlers run only on the main thread, so the example itself calls
# `#with_inner` and a TracePoint sends the signal at an exact point of it
describe Rdkafka::NativeKafka, "#with_inner when a signal trap handler raises" do
  before do
    skip "TracePoint core method events need --debug on JRuby" if defined?(JRUBY_VERSION)

    @producer = rdkafka_producer_config.producer
    @trapped = Class.new(StandardError)

    @producer.queue_size
  end

  after { @producer.close unless @producer.nil? || @producer.closed? }

  # Runs the block and raises from a `USR1` trap handler at each of the given matching events
  def trapped_at(events, calls, matches, before_signal: nil)
    error = @trapped
    previous = trap("USR1") { raise error }
    seen = 0

    trace = TracePoint.new(*events) do |tp|
      next unless matches.call(tp)

      seen += 1
      next unless calls.include?(seen)

      before_signal&.call
      Process.kill("USR1", Process.pid)
      sleep(5)
    end

    assert_raises(error) { trace.enable { yield } }
  ensure
    trap("USR1", previous)
  end

  # Calls `producer.queue_size` and raises from a `USR1` trap handler at each of the given calls
  # of the method that `#with_inner` makes
  def queue_size_trapped_at(klass, method_id, event, calls, before_signal: nil)
    matches = ->(tp) { tp.defined_class == klass && tp.method_id == method_id }

    trapped_at([event, :"c_#{event}"], calls, matches, before_signal: before_signal) do
      @producer.queue_size
    end
  end

  # Runs the block and raises from a `USR1` trap handler at the first run of each of the given
  # lines of `native_kafka.rb`, in order, given as `[stripped line, occurrence]`
  def lines_trapped(*lines, &)
    path = described_class.instance_method(:with_inner).source_location.first
    source = File.readlines(path).map(&:strip)

    linenos = lines.map do |text, occurrence|
      source.each_index.select { |index| source[index] == text }.fetch(occurrence) + 1
    end

    matches = ->(tp) { tp.path == path && linenos.include?(tp.lineno) }

    trapped_at([:line], (1..linenos.size).to_a, matches, &)
  end

  def assert_close_finishes(client = @producer)
    closer = Thread.new { client.close }

    refute_nil(closer.join(5))
  ensure
    closer&.kill
  end

  context "when it raises before the interrupt mask of the release" do
    it "lets close finish" do
      queue_size_trapped_at(Thread.singleton_class, :handle_interrupt, :call, [1])

      assert_close_finishes
    end
  end

  context "when it raises before the lock for the decrement" do
    it "lets close finish" do
      queue_size_trapped_at(Thread::Mutex, :synchronize, :call, [2])

      assert_close_finishes
    end
  end

  context "when it raises before the lock for the decrement while close waits" do
    it "wakes close" do
      closer = nil

      wait_for_close = lambda do
        closer = Thread.new { @producer.close }
        sleep(0.01) until closer.status == "sleep"
      end

      queue_size_trapped_at(Thread::Mutex, :synchronize, :call, [2], before_signal: wait_for_close)

      refute_nil(closer.join(5))
    ensure
      closer&.kill
    end
  end

  context "when it raises after the decrement" do
    it "lets close finish" do
      queue_size_trapped_at(Thread::Mutex, :synchronize, :return, [2])

      assert_close_finishes
    end
  end

  context "when it raises again in the retried release" do
    it "lets close finish" do
      queue_size_trapped_at(Thread::Mutex, :synchronize, :call, [2, 3])

      assert_close_finishes
    end
  end

  context "when it raises before the decrement and again after the retried decrement" do
    it "lets close finish" do
      lines_trapped(["@operations_in_progress -= 1", 1], ["released = true", 1]) do
        @producer.queue_size
      end

      assert_close_finishes
    end
  end

  # `Consumer#close` calls `#with_inner` while it holds the lock
  context "when the call holds the lock" do
    before { @consumer = rdkafka_consumer_config.consumer }

    after { @consumer.close unless @consumer.closed? }

    context "when it raises after the decrement" do
      it "lets close finish" do
        lines_trapped(["released = true", 0]) { @consumer.close }

        assert_close_finishes(@consumer)
      end
    end

    context "when it raises before the decrement and again after the retried decrement" do
      it "lets close finish" do
        lines_trapped(["@operations_in_progress -= 1", 0], ["released = true", 1]) do
          @consumer.close
        end

        assert_close_finishes(@consumer)
      end
    end
  end
end

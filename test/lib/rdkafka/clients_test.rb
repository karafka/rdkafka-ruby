# frozen_string_literal: true

describe Rdkafka::Clients do
  it "closes every registered client" do
    client_class = Class.new do
      attr_reader :closed
      alias_method :closed?, :closed

      def close
        @closed = true
      end
    end
    clients = 3.times.map { described_class.register(client_class.new) }
    GC.compact if GC.respond_to?(:compact)

    described_class.close_all

    assert(clients.all?(&:closed?))
  end

  it "continues closing clients after a failure" do
    client_class = Class.new do
      attr_accessor :error
      attr_reader :closed
      alias_method :closed?, :closed

      def initialize(error = nil)
        @error = error
      end

      def close
        raise error if error
        @closed = true
      end
    end
    error = RuntimeError.new("close failed")
    failing_client = described_class.register(client_class.new(error))
    successful_client = described_class.register(client_class.new)

    raised = assert_raises(RuntimeError) { described_class.close_all }

    assert_same(error, raised)
    assert_predicate(successful_client, :closed?)

    failing_client.error = nil
    described_class.close_all
  end

  unless RUBY_PLATFORM == "java"
    it "starts a fresh registry after fork" do
      client_class = Class.new do
        attr_reader :closed
        alias_method :closed?, :closed

        def initialize
          @closed = false
        end

        def close
          @closed = true
        end
      end
      parent_client = described_class.register(client_class.new)
      reader, writer = IO.pipe

      pid = fork do
        reader.close
        child_client = described_class.register(client_class.new)
        described_class.close_all
        writer.write("#{parent_client.closed?.inspect}:#{child_client.closed?.inspect}")
        writer.close
        exit!(0)
      end

      writer.close
      result = reader.read
      _, status = Process.waitpid2(pid)

      assert_predicate(status, :success?)
      assert_equal("false:true", result)
      refute_predicate(parent_client, :closed?)

      described_class.close_all
    end

    it "destroys live clients before native shutdown" do
      fixture = File.expand_path("../../../spec/fixtures/client_cleanup.rb", __dir__)
      output = IO.popen([RbConfig.ruby, "-Ilib", fixture], err: [:child, :out], &:read)
      status = $?

      assert_predicate(status, :success?, output)
    end
  end
end

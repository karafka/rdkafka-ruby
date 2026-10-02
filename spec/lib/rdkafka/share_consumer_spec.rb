# frozen_string_literal: true

RSpec.describe Rdkafka::ShareConsumer do
  let(:config) { rdkafka_share_consumer_config }
  let(:share_consumer) { config.share_consumer }

  after do
    share_consumer.close unless share_consumer.closed?
  end

  describe "share consumer creation" do
    it "creates a share consumer" do
      expect(share_consumer).to be_a(described_class)
      expect(share_consumer.closed?).to be false
    end

    # Share consumers bypass `Config#build_native_client` (they own the native handle directly
    # instead of a NativeKafka wrapper), so registration has to happen in `Config#share_consumer`.
    # Without it `at_exit { Clients.close_all }` never sees them and librdkafka can be dlclosed
    # with a live share handle - the segfault the at_exit hook exists to prevent.
    it "registers itself so it is closed before Ruby shutdown finalization" do
      allow(Rdkafka::Clients).to receive(:register).and_call_original

      # Referencing the lazy `share_consumer` is what builds it, so the spy has to be set up first.
      built = share_consumer

      expect(Rdkafka::Clients).to have_received(:register).with(built)
    end

    it "raises ClientCreationError for properties librdkafka rejects for share consumers" do
      expect {
        rdkafka_share_consumer_config("enable.auto.commit": true).share_consumer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /share consumer/)
    end

    it "raises ClientCreationError for auto.offset.reset" do
      expect {
        rdkafka_share_consumer_config("auto.offset.reset": "earliest").share_consumer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /share consumer/)
    end

    it "raises ConfigError when a rebalance listener is set" do
      listener_config = rdkafka_share_consumer_config
      listener_config.consumer_rebalance_listener = Object.new

      expect {
        listener_config.share_consumer
      }.to raise_error(Rdkafka::Config::ConfigError, /rebalance/)
    end

    it "accepts the share-specific properties" do
      consumer = rdkafka_share_consumer_config(
        "share.acknowledgement.mode": "explicit",
        "max.poll.records": 100
      ).share_consumer

      expect(consumer.closed?).to be false

      consumer.close
    end

    # Regression test for the bug where ShareConsumer#name was nil for the whole lifetime of a
    # share consumer unless OAuthBearer happened to be configured, which silently dropped every
    # share-consumer statistic and background error for downstreams (e.g. Karafka) that route the
    # global callbacks by matching the client name. The name must be derived from the native
    # handle like Consumer#name - available immediately, before any poll and without OAuth.
    it "exposes the librdkafka client name immediately after creation" do
      # Must be the real client name (e.g. "rdkafka#consumer-1"), matching what the statistics
      # and error callbacks carry - not the garbage rd_kafka_name returns for a raw share handle.
      expect(share_consumer.name).to match(/\A\S+#consumer-\d+\z/)
    end

    it "keeps reporting its name after it is closed" do
      name = share_consumer.name
      share_consumer.close

      expect(share_consumer.name).to eq(name)
    end
  end

  describe "#subscribe, #subscription and #unsubscribe" do
    it "raises ArgumentError for an empty subscribe instead of silently unsubscribing" do
      expect {
        share_consumer.subscribe
      }.to raise_error(ArgumentError, /empty subscribe/)
    end

    it "subscribes to topics and reads back the subscription" do
      share_consumer.subscribe("topic-a", "topic-b")

      expect(share_consumer.subscription).to be_a(Rdkafka::Consumer::TopicPartitionList)
      expect(share_consumer.subscription.to_h.keys).to contain_exactly("topic-a", "topic-b")

      share_consumer.unsubscribe

      expect(share_consumer.subscription.to_h).to be_empty
    end
  end

  describe "#poll" do
    it "returns an empty array when there are no messages" do
      share_consumer.subscribe(TestTopics.non_existing)

      expect(share_consumer.poll(100)).to eq([])
    end

    it "raises when polling without a subscription" do
      expect {
        share_consumer.poll(100)
      }.to raise_error(Rdkafka::RdkafkaError, /subscribed/)
    end
  end

  describe "#events_poll and #events_poll_nb" do
    it "returns an event count without acquiring records when there is nothing to serve" do
      # Without a subscription nothing is fetched; events_poll just drains the main queue and
      # returns the number of events served (0 here), never records.
      expect(share_consumer.events_poll(0)).to be_a(Integer)
      expect(share_consumer.events_poll_nb(0)).to be_a(Integer)
    end

    it "does not raise when called without a subscription (unlike #poll)" do
      expect { share_consumer.events_poll(0) }.not_to raise_error
      expect { share_consumer.events_poll_nb(0) }.not_to raise_error
    end

    it "keeps the statistics callback firing while only events_poll is called (no record polling)" do
      received = []
      Rdkafka::Config.statistics_callback = ->(stats) { received << stats }

      # Rebuild with a low statistics interval so a callback is due within the loop below
      consumer = rdkafka_share_consumer_config("statistics.interval.ms": 100).share_consumer
      name = consumer.name

      begin
        consumer.subscribe(TestTopics.non_existing)

        # Never call #poll: only events_poll services the main queue here. If the statistics
        # callback still fires it proves events_poll drains the callbacks independently of
        # record acquisition.
        50.times do
          consumer.events_poll(100)
          break if received.any? { |s| s["name"] == name }
        end

        expect(received).not_to be_empty
        expect(received.map { |s| s["name"] }).to include(name)
      ensure
        consumer.close
        Rdkafka::Config.statistics_callback = nil
      end
    end
  end

  describe "#acknowledge" do
    let(:message) do
      instance_double(
        Rdkafka::ShareConsumer::Message,
        topic: "topic",
        partition: 0,
        offset: 0
      )
    end

    it "raises ArgumentError for an unknown acknowledge type" do
      expect {
        share_consumer.acknowledge(message, :nack)
      }.to raise_error(ArgumentError, /Unknown acknowledge type/)
    end

    it "raises RdkafkaError when acknowledging a record that was never delivered" do
      expect {
        share_consumer.acknowledge(message)
      }.to raise_error(Rdkafka::RdkafkaError)
    end
  end

  describe "#commit_sync and #commit_async" do
    it "allows committing when there is nothing to commit" do
      expect { share_consumer.commit_async }.not_to raise_error
      expect { share_consumer.commit_sync }.not_to raise_error
    end
  end

  describe "#acknowledgement_commit_callback=" do
    it "raises TypeError for a non-callable callback" do
      expect {
        share_consumer.acknowledgement_commit_callback = "not callable"
      }.to raise_error(TypeError)
    end

    it "accepts a callable and can be cleared with nil" do
      share_consumer.acknowledgement_commit_callback = ->(_results, _error) {}
      share_consumer.acknowledgement_commit_callback = nil
    end
  end

  describe "#close and #closed?" do
    it "closes the consumer and marks it closed" do
      share_consumer.close

      expect(share_consumer.closed?).to be true
    end

    it "allows closing more than once" do
      share_consumer.close

      expect { share_consumer.close }.not_to raise_error
    end

    it "treats a share consumer inherited across fork as closed in the child, leaving teardown to the parent", skip: defined?(JRUBY_VERSION) && "Kernel#fork is not available" do
      # librdkafka is not fork-safe: `fork` copies only the calling thread, so the broker/main
      # threads backing this handle do not exist in the child. An inherited share handle must
      # therefore report as closed in the child and `#close` must be a no-op there - never calling
      # `rd_kafka_share_destroy` on threads that no longer exist. Otherwise the child crashes
      # (SIGSEGV) when Ruby runs the inherited consumer's GC finalizer on exit.
      share_consumer # force creation in the parent so the child inherits a live, open handle

      pid = fork do
        # In the child the inherited handle belongs to another process. Exit 0 only when it reports
        # closed, its #close is a no-op leaving it closed, and a handle-touching call is rejected
        # (rather than dereferencing the inherited handle).
        inherited_reports_closed = share_consumer.closed?
        share_consumer.close
        rejected = begin
          share_consumer.poll(0)
          false
        rescue Rdkafka::ClosedConsumerError
          true
        end
        exit((inherited_reports_closed && share_consumer.closed? && rejected) ? 0 : 1)
      end

      _, status = Process.wait2(pid)

      expect(status.signaled?).to be(false) # a SIGSEGV here would mean the guard let the child destroy the handle
      expect(status.exitstatus).to eq(0)

      # The parent created the handle, so it is unaffected: still open and usable.
      expect(share_consumer.closed?).to be(false)
    end

    it "waits for an in-flight poll from another thread instead of crashing" do
      share_consumer.subscribe(TestTopics.non_existing)

      poller = Thread.new do
        loop { share_consumer.poll(200) }
      rescue Rdkafka::ClosedConsumerError, Rdkafka::RdkafkaError
        :done
      end

      sleep(0.2)
      share_consumer.close

      expect(poller.join(15)&.value).to eq(:done)
      expect(share_consumer.closed?).to be true
    end

    it "raises ClosedConsumerError for public methods after close" do
      share_consumer.close

      expect { share_consumer.subscribe("topic") }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.unsubscribe }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.subscription }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.poll(0) }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.events_poll(0) }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.events_poll_nb(0) }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.acknowledge(nil) }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.commit_sync }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.commit_async }.to raise_error(Rdkafka::ClosedConsumerError)
      expect { share_consumer.acknowledgement_commit_callback = nil }.to raise_error(Rdkafka::ClosedConsumerError)
    end
  end

  describe "consuming with a share group" do
    let(:topic) { TestTopics.create(partitions: 2) }
    let(:group_id) { share_group_id }
    let(:share_consumer) { rdkafka_share_consumer_config("group.id": group_id).share_consumer }
    let(:producer) { rdkafka_producer_config.producer }

    after { producer.close }

    # share.auto.offset.reset is a broker-side group config (not a client property) and
    # defaults to latest, so it has to be set before the group first attaches to the
    # partitions for pre-produced records to be delivered
    def reset_share_group_to_earliest
      admin = rdkafka_config.admin
      admin.incremental_alter_configs(
        [
          {
            resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
            resource_name: group_id,
            configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
          }
        ]
      ).wait(max_wait_timeout_ms: 15_000)
      admin.close
    end

    def produce_and_wait(count, prefix)
      handles = count.times.map { |i| producer.produce(topic: topic, payload: "#{prefix}-#{i}") }
      handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
    end

    it "consumes produced messages with delivery counts" do
      reset_share_group_to_earliest

      handles = 10.times.map do |i|
        producer.produce(topic: topic, payload: "share-payload-#{i}", key: "share-key-#{i}")
      end
      handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }

      share_consumer.subscribe(topic)

      messages = []
      30.times do
        messages.concat(share_consumer.poll(1_000))
        break if messages.size >= 10
      end

      expect(messages.size).to eq(10)
      expect(messages).to all(be_a(Rdkafka::ShareConsumer::Message))
      expect(messages.map(&:payload)).to match_array(10.times.map { |i| "share-payload-#{i}" })
      expect(messages.map(&:delivery_count)).to all(eq(1))
      expect(messages.none?(&:error?)).to be true
      expect(messages.first.timestamp).to be_a(Time)
    end

    context "when several async commits queue up for the same partition" do
      let(:topic) { TestTopics.create(partitions: 1) }
      let(:share_consumer) do
        rdkafka_share_consumer_config(
          "group.id": group_id,
          "share.acknowledgement.mode": "explicit"
        ).share_consumer
      end

      it "merges their acknowledgements in ascending offset order so the broker accepts them" do
        reset_share_group_to_earliest

        handles = 20.times.map { |i| producer.produce(topic: topic, payload: "payload-#{i}") }
        handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }

        results = Queue.new
        share_consumer.acknowledgement_commit_callback = ->(offsets, error) { results << [offsets, error] }
        share_consumer.subscribe(topic)

        # In explicit mode every delivered record must be acknowledged before the next poll, so
        # only the first non-empty batch is used
        messages = []
        30.times do
          messages = share_consumer.poll(1_000)
          break unless messages.empty?
        end

        expect(messages.size).to be >= 8

        # While the first async commit is in flight, the following ones are merged into the
        # batch pending for the partition. Acknowledging every fourth record per commit makes
        # those merged ranges interleave.
        messages.group_by.with_index { |_, i| i % 4 }.each_value do |slice|
          slice.each { |message| share_consumer.acknowledge(message, :accept) }
          share_consumer.commit_async
        end
        share_consumer.commit_sync

        acknowledged = []
        errors = []
        unsorted = []
        20.times do
          share_consumer.poll(500)

          until results.empty?
            offsets, error = results.pop
            errors << error if error
            offsets.each do |partition_offsets|
              acknowledged.concat(partition_offsets[:offsets])
              unsorted << partition_offsets[:offsets] unless partition_offsets[:offsets] == partition_offsets[:offsets].sort
            end
          end

          break if errors.any? || acknowledged.size >= messages.size
        end

        expect(errors).to be_empty
        expect(unsorted).to be_empty
        expect(acknowledged).to match_array(messages.map(&:offset))
      end
    end

    context "when unsubscribing while a share fetch is in flight" do
      let(:topic) { TestTopics.create(partitions: 1) }
      let(:member_config) { { "group.id": group_id, "share.acknowledgement.mode": "explicit" } }
      let(:share_consumer) { rdkafka_share_consumer_config(member_config).share_consumer }
      let(:other_consumer) { rdkafka_share_consumer_config(member_config).share_consumer }

      before { reset_share_group_to_earliest }

      after { other_consumer.close }

      it "releases what the fetch acquires so another member gets every record on its first delivery" do
        produce_and_wait(1, "first")

        share_consumer.subscribe(topic)
        first = []
        30.times do
          first = share_consumer.poll(500)
          break unless first.empty?
        end
        expect(first.size).to eq(1)
        first.each { |message| share_consumer.acknowledge(message, :accept) }
        share_consumer.commit_sync
        # Nothing is left to fetch, so a long-polling ShareFetch stays in flight
        2.times { share_consumer.poll(500) }

        other_consumer.subscribe(topic)
        2.times { other_consumer.poll(300) }

        share_consumer.unsubscribe
        produce_and_wait(20, "payload")

        started = Time.now
        delivery_counts = []
        while delivery_counts.size < 20 && Time.now - started < 25
          other_consumer.poll(500).each do |message|
            delivery_counts << message.delivery_count
            other_consumer.acknowledge(message, :accept)
          end
        end

        # Without releasing them, records acquired by the in-flight fetch reach the other member
        # only once their acquisition lock (30s by default) expires, as redeliveries
        expect(delivery_counts).to eq([1] * 20)
        expect(Time.now - started).to be < 10
      end

      it "keeps fetching and acknowledging records after subscribing again" do
        produce_and_wait(5, "before")

        share_consumer.subscribe(topic)
        first = []
        30.times do
          first = share_consumer.poll(500)
          break unless first.empty?
        end
        expect(first).not_to be_empty
        first.each { |message| share_consumer.acknowledge(message, :accept) }
        share_consumer.commit_sync

        share_consumer.unsubscribe
        share_consumer.subscribe(topic)
        produce_and_wait(5, "after")

        callback_errors = []
        share_consumer.acknowledgement_commit_callback = ->(_offsets, error) { callback_errors << error if error }

        received = []
        60.times do
          share_consumer.poll(500).each do |message|
            received << message
            share_consumer.acknowledge(message, :accept)
          end
          break if received.map(&:payload).count { |payload| payload.start_with?("after") } == 5
        end

        expect(received.map(&:payload)).to include(*5.times.map { |i| "after-#{i}" })
        expect(received.map(&:delivery_count)).to all(eq(1))

        results = share_consumer.commit_sync
        expect((results&.to_h || {}).values.flatten.map(&:err)).to all(eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR))

        5.times { share_consumer.poll(200) }
        expect(callback_errors).to be_empty
      end
    end
  end
end

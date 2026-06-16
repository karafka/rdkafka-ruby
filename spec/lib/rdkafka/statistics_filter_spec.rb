# frozen_string_literal: true

RSpec.describe Rdkafka::Config do
  describe "statistics.unassigned.include" do
    let(:stats) { [] }

    before do
      described_class.statistics_callback = ->(published) { stats << published }
    end

    after do
      described_class.statistics_callback = nil
    end

    def wait_for_stats(count = 1, timeout: 5)
      (timeout * 20).times do
        break if stats.size >= count
        sleep 0.05
      end
    end

    context "when set to false for a producer" do
      let(:producer) do
        rdkafka_producer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": false
        ).producer
      end

      after { producer.close }

      it "accepts the config without error" do
        expect(producer).to be_a(Rdkafka::Producer)
      end

      it "still emits statistics with root-level keys" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats

        stat = stats.last
        expect(stat).to have_key("name")
        expect(stat).to have_key("type")
        expect(stat).to have_key("ts")
        expect(stat).to have_key("brokers")
        expect(stat).to have_key("topics")
        expect(stat["type"]).to eq("producer")
      end

      it "emits empty topics section" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats

        expect(stats.last["topics"]).to be_empty
      end

      it "preserves root-level aggregate totals" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(2)

        stat = stats.last
        expect(stat["txmsgs"]).to be_a(Integer)
        expect(stat["rxmsgs"]).to be_a(Integer)
      end

      it "preserves broker statistics" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats

        brokers = stats.last["brokers"]
        expect(brokers).not_to be_empty
        broker = brokers.values.first
        expect(broker).to have_key("tx")
        expect(broker).to have_key("rx")
      end

      it "emits empty brokers.toppars for all brokers" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(2)

        stat = stats.reverse.find { |s| s["brokers"].any? { |_, b| b["toppars"] } }
        expect(stat).not_to be_nil

        stat["brokers"].each_value do |broker|
          expect(broker).to have_key("toppars")
          expect(broker["toppars"]).to be_empty
        end
      end
    end

    context "when set to false for a consumer" do
      let(:consumer) do
        rdkafka_consumer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": false
        ).consumer
      end

      after { consumer.close }

      def poll_until_stats(consumer, count = 1, timeout: 5)
        (timeout * 20).times do
          break if stats.size >= count
          consumer.poll(50)
        end
      end

      it "accepts the config without error" do
        expect(consumer).to be_a(Rdkafka::Consumer)
      end

      it "still emits statistics with root-level keys" do
        consumer.subscribe("test")
        poll_until_stats(consumer)

        stat = stats.last
        expect(stat).to have_key("name")
        expect(stat).to have_key("type")
        expect(stat).to have_key("brokers")
        expect(stat["type"]).to eq("consumer")
      end

      it "preserves consumer group statistics" do
        consumer.subscribe("test")
        poll_until_stats(consumer, 2)

        stat = stats.reverse.find { |s| s["cgrp"] }
        expect(stat).not_to be_nil
        expect(stat["cgrp"]).to have_key("state")
      end

      it "preserves topic data for consumers" do
        consumer.subscribe("test")
        # Consumer needs time to join group, get partitions assigned, and start
        # fetching before topics appear in filtered stats
        (30 * 20).times do
          break if stats.any? { |s| !s["topics"].empty? }
          consumer.poll(50)
        end

        stat = stats.reverse.find { |s| !s["topics"].empty? }
        expect(stat).not_to be_nil
        expect(stat["topics"]).to have_key("test")
      end

      it "filters brokers.toppars to only actively fetching partitions" do
        consumer.subscribe("test")
        # Wait until partitions are assigned and the consumer is fetching
        (30 * 20).times do
          break if stats.any? { |s| !s["topics"].empty? }
          consumer.poll(50)
        end

        stat = stats.reverse.find { |s| !s["topics"].empty? }
        expect(stat).not_to be_nil

        # Each broker entry should have a toppars key; entries present must
        # belong to the subscribed topic (no unassigned partitions leaking).
        stat["brokers"].each_value do |broker|
          expect(broker).to have_key("toppars")
          broker["toppars"].each_value do |tp|
            expect(tp["topic"]).to eq("test")
          end
        end
      end
    end

    context "when set to true (default behavior) for a producer" do
      let(:producer) do
        rdkafka_producer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": true
        ).producer
      end

      after { producer.close }

      it "emits topics with partitions" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(2)

        stat = stats.reverse.find { |s| !s["topics"].empty? }
        expect(stat).not_to be_nil
        expect(stat["topics"]).to have_key("test")
        expect(stat["topics"]["test"]["partitions"]).not_to be_empty
      end

      it "preserves root-level keys and broker data" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(2)

        stat = stats.last
        expect(stat).to have_key("name")
        expect(stat).to have_key("ts")
        expect(stat).to have_key("brokers")
        expect(stat["brokers"]).not_to be_empty
        expect(stat["txmsgs"]).to be_a(Integer)
        expect(stat["rxmsgs"]).to be_a(Integer)
      end

      it "populates brokers.toppars for the produced topic" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(3)

        # At least one broker should report our test topic in its toppars map
        has_test_toppar = stats.any? do |s|
          s["brokers"].any? do |_, b|
            (b["toppars"] || {}).any? { |_, tp| tp["topic"] == "test" }
          end
        end
        expect(has_test_toppar).to be(true)
      end
    end

    context "when set to true (default behavior) for a consumer" do
      let(:consumer) do
        rdkafka_consumer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": true
        ).consumer
      end

      after { consumer.close }

      def poll_until(consumer, timeout: 30)
        (timeout * 20).times do
          break if yield
          consumer.poll(50)
        end
      end

      it "emits topics with partitions" do
        consumer.subscribe("test")
        poll_until(consumer) { stats.any? { |s| !s["topics"].empty? } }

        stat = stats.reverse.find { |s| !s["topics"].empty? }
        expect(stat).not_to be_nil
        expect(stat["topics"]).to have_key("test")
        expect(stat["topics"]["test"]["partitions"]).not_to be_empty
      end

      it "preserves consumer group statistics" do
        consumer.subscribe("test")
        poll_until(consumer) { stats.any? { |s| s["cgrp"] } }

        stat = stats.reverse.find { |s| s["cgrp"] }
        expect(stat).not_to be_nil
        expect(stat["cgrp"]).to have_key("state")
      end

      it "preserves root-level keys and broker data" do
        consumer.subscribe("test")
        poll_until(consumer) { stats.size >= 2 }

        stat = stats.last
        expect(stat).to have_key("name")
        expect(stat).to have_key("type")
        expect(stat).to have_key("brokers")
        expect(stat["brokers"]).not_to be_empty
        expect(stat["type"]).to eq("consumer")
        expect(stat["rxmsgs"]).to be_a(Integer)
      end

      it "populates brokers.toppars for the subscribed topic" do
        consumer.subscribe("test")
        poll_until(consumer) do
          stats.any? do |s|
            s["brokers"].any? do |_, b|
              (b["toppars"] || {}).any? { |_, tp| tp["topic"] == "test" }
            end
          end
        end

        stat = stats.reverse.find do |s|
          s["brokers"].any? do |_, b|
            (b["toppars"] || {}).any? { |_, tp| tp["topic"] == "test" }
          end
        end

        expect(stat).not_to be_nil
      end
    end

    context "when config is omitted (default behavior)" do
      let(:producer) do
        rdkafka_producer_config("statistics.interval.ms": 100).producer
      end

      after { producer.close }

      it "includes all topics and partitions by default" do
        producer.produce(topic: "test", payload: "test").wait
        wait_for_stats(2)

        stat = stats.reverse.find { |s| !s["topics"].empty? }
        expect(stat).not_to be_nil
        expect(stat["topics"]).to have_key("test")
        expect(stat["topics"]["test"]["partitions"]).not_to be_empty
      end
    end

    context "when comparing JSON size with many partitions" do
      let(:big_topic) { "stats-filter-big-#{SecureRandom.hex(6)}" }
      let(:admin) { rdkafka_config.admin }

      before do
        admin.create_topic(big_topic, 1_000, 1).wait(max_wait_timeout_ms: 15_000)
        # `wait` only confirms the create request was accepted; leader election and metadata
        # propagation for 1_000 partitions continue afterwards. Give the cluster a moment to
        # settle so the partition metadata is visible to the consumers/producers below.
        sleep(1)
      end

      after do
        unless admin.closed?
          begin
            admin.delete_topic(big_topic).wait(max_wait_timeout_ms: 15_000)
          rescue Rdkafka::RdkafkaError
            nil
          end
        end

        admin.close
      end

      it "produces significantly smaller statistics JSON for producers" do
        # Close the admin to prevent its unfiltered stats from leaking
        # into our collection via the global callback
        admin.close

        filtered_stats = []
        unfiltered_stats = []

        described_class.statistics_callback = ->(published) { filtered_stats << published }

        filtered_producer = rdkafka_producer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": false
        ).producer
        filtered_producer.produce(topic: big_topic, payload: "test").wait

        wait = ->(target) {
          (10 * 20).times do
            break if target.size >= 2
            sleep 0.05
          end
        }

        wait.call(filtered_stats)
        filtered_producer.close

        described_class.statistics_callback = ->(published) { unfiltered_stats << published }

        unfiltered_producer = rdkafka_producer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": true
        ).producer
        unfiltered_producer.produce(topic: big_topic, payload: "test").wait
        wait.call(unfiltered_stats)
        unfiltered_producer.close

        filtered_json = JSON.generate(filtered_stats.last)
        unfiltered_json = JSON.generate(unfiltered_stats.last)

        expect(filtered_json.bytesize).to be < (unfiltered_json.bytesize / 2)
      end

      it "produces significantly smaller statistics JSON for consumers" do
        # Close the admin to prevent its unfiltered stats from leaking
        # into our collection via the global callback
        admin.close

        filtered_stats = []
        unfiltered_stats = []

        poll_until = ->(consumer, target, &condition) {
          # Up to ~60s: metadata for a freshly-created 1_000-partition topic can take a while to
          # propagate into librdkafka's statistics on a loaded CI runner.
          (60 * 20).times do
            break if condition.call(target)
            begin
              consumer.poll(50)
            rescue Rdkafka::RdkafkaError
              nil
            end
          end
        }

        # Unfiltered first so we can wait for topic metadata to appear
        described_class.statistics_callback = ->(published) { unfiltered_stats << published }

        unfiltered_consumer = rdkafka_consumer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": true
        ).consumer
        unfiltered_consumer.subscribe(big_topic)
        has_partitions = ->(s) {
          s.any? { |stat| (stat["topics"][big_topic] || {}).fetch("partitions", {}).size > 100 }
        }
        poll_until.call(unfiltered_consumer, unfiltered_stats, &has_partitions)
        unfiltered_consumer.close

        described_class.statistics_callback = ->(published) { filtered_stats << published }

        filtered_consumer = rdkafka_consumer_config(
          "statistics.interval.ms": 100,
          "statistics.unassigned.include": false
        ).consumer
        filtered_consumer.subscribe(big_topic)
        enough_stats = ->(s) { s.size >= 2 }
        poll_until.call(filtered_consumer, filtered_stats, &enough_stats)
        filtered_consumer.close

        filtered_json = JSON.generate(filtered_stats.last)
        unfiltered_stat = unfiltered_stats.reverse.find do |s|
          (s["topics"][big_topic] || {}).fetch("partitions", {}).size > 100
        end

        # Guard the precondition explicitly: without it a missing snapshot makes
        # JSON.generate(nil) return "null" and the assertion fails as the cryptic `expected: < 2`.
        expect(unfiltered_stat).not_to(
          be_nil,
          "no statistics snapshot with >100 partitions arrived in time; the 1_000-partition " \
          "topic metadata likely had not propagated yet"
        )

        unfiltered_json = JSON.generate(unfiltered_stat)

        expect(filtered_json.bytesize).to be < (unfiltered_json.bytesize / 2)
      end
    end
  end
end

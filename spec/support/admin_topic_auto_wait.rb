# frozen_string_literal: true

# Auto-waits for topic metadata propagation after a successful
# +Rdkafka::Admin#create_topic(...).wait+.
#
# Broker acknowledgment of topic creation does not guarantee that the topic is
# immediately visible to subsequent metadata queries, especially on slow CI
# runners. Every spec that creates a topic and then queries metadata needed to
# remember to call +wait_for_topic+ manually; forgetting it produced sporadic
# +unknown_topic_or_part+ failures.
#
# This patch wraps the handle returned by +create_topic+ so that a successful
# +.wait+ transparently polls metadata until the topic is visible. Error paths
# (raised exceptions or non-zero response codes) are left untouched.
module AdminTopicAutoWait
  # Thin delegating wrapper that augments +#wait+ with a post-success metadata
  # poll. All other calls are forwarded to the underlying handle unchanged so
  # existing expectations against the handle (e.g. +create_result+,
  # +operation_name+, FFI struct fields) continue to work.
  class HandleWrapper
    def initialize(handle, admin, topic_name)
      @handle = handle
      @admin = admin
      @topic_name = topic_name
    end

    def wait(*args, **kwargs)
      result = @handle.wait(*args, **kwargs)

      # Only poll metadata if the underlying operation actually succeeded.
      # Callers using +raise_response_error: false+ may receive a result on
      # failure, in which case there is no topic to wait for.
      if @handle[:response] == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
        KafkaWaitHelpers.wait_for_topic(@admin, @topic_name)
      end

      result
    end

    def respond_to_missing?(name, include_private = false)
      @handle.respond_to?(name, include_private) || super
    end

    def method_missing(name, *args, **kwargs, &block)
      if @handle.respond_to?(name)
        @handle.public_send(name, *args, **kwargs, &block)
      else
        super
      end
    end
  end

  def create_topic(topic_name, *args, **kwargs)
    handle = super
    HandleWrapper.new(handle, self, topic_name)
  end
end

Rdkafka::Admin.prepend(AdminTopicAutoWait)

# File Descriptor API for Fiber Scheduler Integration

## Changes Made

### 1. FFI Bindings (`lib/rdkafka/bindings.rb`)

Added three new FFI function bindings to expose librdkafka's queue FD operations:

```ruby
attach_function :rd_kafka_queue_get_background, [:pointer], :pointer
attach_function :rd_kafka_queue_get_fd, [:pointer], :int
attach_function :rd_kafka_queue_io_event_enable, [:pointer, :int], :void
```

### 2. NativeKafka API (`lib/rdkafka/native_kafka.rb`)

Added two new methods to expose file descriptors:

```ruby
# Returns the file descriptor for the main queue
# The main queue contains consumer messages if consumer_poll_set is true (default),
# or producer/admin events and statistics
def main_queue_fd
  with_inner do |inner|
    queue_ptr = Bindings.rd_kafka_queue_get_main(inner)
    Bindings.rd_kafka_queue_get_fd(queue_ptr)
  end
end

# Returns the file descriptor for the background queue
# The background queue contains background events and statistics
def background_queue_fd
  with_inner do |inner|
    queue_ptr = Bindings.rd_kafka_queue_get_background(inner)
    Bindings.rd_kafka_queue_get_fd(queue_ptr)
  end
end
```

### 3. Tests

Added comprehensive tests in:
- `spec/lib/rdkafka/native_kafka_spec.rb` - Unit tests for FD access
- `spec/lib/rdkafka/consumer_spec.rb` - Integration tests with IO.select

## File Descriptor Lifecycle

**You do NOT need to close the FD yourself.** Here's why:

1. **FDs are just numbers** - `rd_kafka_queue_get_fd()` returns a file descriptor number (integer) associated with the queue pointer. You're not creating or taking ownership of the FD.

2. **librdkafka manages the FD** - The FD is managed by librdkafka internally. It will be closed automatically when:
   - The queue is destroyed
   - The Kafka client (NativeKafka) is destroyed/closed

3. **Safe to read** - You can call `main_queue_fd` and `background_queue_fd` multiple times. Each call just retrieves the current FD number. The FD itself remains active throughout the Kafka client's lifetime.

4. **Raises on closed client** - If you try to access the FD after closing the Kafka client, it raises `ClosedInnerError`, preventing accidental use of invalid FDs.

## Usage Example: Fiber Scheduler Integration

```ruby
require 'rdkafka'

# Create consumer
config = Rdkafka::Config.new(
  :"bootstrap.servers" => "localhost:9092",
  :"group.id" => "my-group"
)
consumer = config.consumer
consumer.subscribe("my-topic")

# Get the file descriptor for the queue
fd = consumer.native_kafka.main_queue_fd

# Use with IO.select (or any event loop)
loop do
  # Wait for messages
  readable, = IO.select([fd], nil, nil, timeout)

  if readable
    # Poll without blocking - FD told us there's data
    while msg = consumer.poll(0)
      puts msg.payload
    end
  end
end
```

## Benefits for Karafka/Waterdrop

1. **Zero-copy polling** - Direct access to librdkafka's queues
2. **Fiber scheduler compatible** - Works with Falcon, Async, etc.
3. **No background threads** - You control the polling loop
4. **Low overhead** - Minimal Ruby wrapper, just FD access
5. **Familiar APIs** - Uses standard `IO.select`, `IO.wait_readable`, etc.

## Advanced: Background Queue for Events

The background queue contains statistics and admin events:

```ruby
# For producer or admin clients
producer = config.producer
bg_fd = producer.native_kafka.background_queue_fd

# Monitor for events like delivery confirmations
readable, = IO.select([bg_fd], nil, nil, 0.1)
if readable
  # Events are processed via callbacks
end
```

## Notes

- Queues are created automatically by librdkafka - you don't need to create them
- The `consumer_poll_set: true` (default) redirects the main queue to contain consumer messages
- Multiple calls to `main_queue_fd` return the same FD number
- FD is only valid while the client is open

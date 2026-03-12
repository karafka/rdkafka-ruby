# frozen_string_literal: true

# Simple thread double that records interactions without strict mock expectations.
# This replaces RSpec's `double(Thread)` with `allow(thread).to receive(...)` permissive stubs.
class ThreadDouble
  attr_accessor :name, :closing, :abort_on_exception

  def initialize
    @store = {}
    @joined = false
  end

  def []=(key, val)
    @store[key] = val
  end

  def [](key)
    @store[key]
  end

  def join
    @joined = true
  end

  def joined?
    @joined
  end
end

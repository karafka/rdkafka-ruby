# frozen_string_literal: true

# This integration test verifies that `PartitionsCountCache` adopts a lower partition count once
# the cached entry has expired - the situation a topic deleted and recreated with fewer partitions
# produces.
#
# The cache prioritizes higher counts (partition counts only grow during normal operation), but it
# used to do so unconditionally: after the TTL expired it fetched the true (lower) count, discarded
# it, re-armed the TTL on the stale higher count, and kept doing so forever. With a stale higher
# count the partitioner picks partition indices >= the real count and `produce` fails with
# `unknown_partition` until the process restarts. The cache now adopts a lower count on the first
# refresh after expiry, while still ignoring lower values within the TTL window (race protection).
#
# This drives the cache directly with a real monotonic clock and a short TTL - no broker needed,
# since the bug is entirely in the cache's get/set logic.
#
# Exit codes:
# - 0: the lower count was adopted after the TTL expired (and ignored while fresh)
# - 1: the cache kept reporting the stale higher count (bug still present)

require "rdkafka"

$stdout.sync = true

TTL_MS = 200
HIGHER = 12
LOWER = 4

cache = Rdkafka::Producer::PartitionsCountCache.new(ttl_ms: TTL_MS)
topic = "partitions-count-cache-probe"

# Seed the higher (original) count.
cache.get(topic) { HIGHER }

# While the entry is fresh, a lower read must NOT displace the higher count (the block is not even
# invoked within the TTL window).
fresh = cache.get(topic) { LOWER }
if fresh != HIGHER
  warn "FAIL: a fresh cache entry returned #{fresh}, expected the cached #{HIGHER}"
  exit(1)
end

# Let the entry expire, then an authoritative refresh returns the lower (recreated) count.
sleep((TTL_MS / 1000.0) + 0.1)
refreshed = cache.get(topic) { LOWER }

if refreshed != LOWER
  warn "FAIL: after TTL expiry the cache returned #{refreshed}, expected the recreated #{LOWER}"
  exit(1)
end

# The adopted lower count must now stick (and be served without another fetch).
sticky = cache.get(topic) { raise "should not refetch within TTL" }
if sticky != LOWER
  warn "FAIL: the adopted lower count did not stick (got #{sticky})"
  exit(1)
end

puts "PASS: cache ignored the lower count while fresh and adopted it after TTL expiry"
exit(0)

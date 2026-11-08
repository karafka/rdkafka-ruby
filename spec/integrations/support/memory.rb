# frozen_string_literal: true

# Helpers for leak specs that sample the process RSS

# @return [Integer] resident set size of this process in KB, read from `/proc/self/status`
def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

# Settles the Ruby heap before sampling RSS. `GC.compact` defragments the heap so freed pages can be
# released, which keeps the baseline and final samples from drifting purely due to fragmentation.
# A plain `GC.start` leaves that drift in, which is what made leak specs trip on Ruby 4.0.
def settle_heap
  GC.start
  GC.compact if GC.respond_to?(:compact)
end

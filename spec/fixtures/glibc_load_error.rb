# frozen_string_literal: true

# Simulates loading a precompiled librdkafka that needs a newer glibc than the host provides
require "ffi"

module FFI
  module Library
    def ffi_lib(*)
      raise LoadError, "/lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found"
    end
  end
end

begin
  load File.expand_path("../../lib/rdkafka.rb", __dir__)
rescue Rdkafka::LibraryLoadError => e
  exit(e.message.include?("requires glibc version 2.38 or higher"))
end

exit 1

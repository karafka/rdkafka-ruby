# frozen_string_literal: true

module Rdkafka
  class Consumer
    # Creates FFI pointers from the process-global allocator so that memory handed to librdkafka
    # can be released by it with the matching `free`.
    module Allocator
      extend FFI::Library

      ffi_lib FFI::CURRENT_PROCESS # use process-global malloc not FFI::Library::LIBC
      attach_function :malloc, [:size_t], :pointer

      class << self
        # Return an FFI pointer to a string allocated from the global allocator
        # so that it can be released by another library.
        #
        # @private
        #
        # @param string [String] Ruby string that will be copied into the new
        #   string pointer.
        #
        # @return [FFI::Pointer]
        def string_pointer(string)
          bytes = string.to_s
          pointer = malloc(bytes.bytesize + 1)
          Kernel.raise(NoMemoryError, "malloc failed") if pointer.null?

          pointer.put_bytes(0, bytes)
          pointer.put_char(bytes.bytesize, 0) # NUL terminator

          pointer.autorelease = false

          pointer
        end
      end
    end
  end
end

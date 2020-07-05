require "spec_helper"

describe Rdkafka::Callbacks do

  # The code in the call back functions is 100% covered by other specs.  Due to
  # the large number of collaborators, and the fact that FFI does not play
  # nicely with doubles, it was very difficult to construct tests that were
  # not over-mocked.

  # For debugging purposes, if you suspect that you are running into trouble in
  # one of the callback functions, it may be helpful to surround the inner body
  # of the method with something like:
  #
  #   begin
  #     <method body>
  #   rescue => ex; puts ex.inspect; puts ex.backtrace; end;
  #
  # This will output to STDOUT any exceptions that are being raised in the callback.

end

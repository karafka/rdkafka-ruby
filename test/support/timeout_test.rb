# frozen_string_literal: true

# 90-second timeout per test
module TimeoutTest
  def run(...)
    Timeout.timeout(90) { super }
  end
end
Minitest::Test.prepend(TimeoutTest)

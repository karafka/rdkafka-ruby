# frozen_string_literal: true

# Add described_class support to Minitest::Spec (like RSpec)
# Walks up the ancestor chain to find the first describe block that was passed a Class
class Minitest::Spec
  def described_class
    self.class.ancestors.each do |ancestor|
      next unless ancestor.respond_to?(:desc)

      return ancestor.desc if ancestor.desc.is_a?(Class)
    end

    nil
  end
end

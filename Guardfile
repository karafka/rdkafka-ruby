# frozen_string_literal: true

logger level: :error

guard :rspec, cmd: "bundle exec rspec --format #{ENV.fetch("FORMAT", "documentation")}" do
  require "guard/rspec/dsl"
  dsl = Guard::RSpec::Dsl.new(self)

  # Ruby files
  ruby = dsl.ruby
  dsl.watch_spec_files_for(ruby.lib_files)
  watch(%r{^lib/(.+)\.rb}) { |m| "spec/#{m[1]}_spec.rb" }

  # RSpec files
  rspec = dsl.rspec
  watch(rspec.spec_helper) { rspec.spec_dir }
  watch(rspec.spec_support) { rspec.spec_dir }
  watch(rspec.spec_files)
end

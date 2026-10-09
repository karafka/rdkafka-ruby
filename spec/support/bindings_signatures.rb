# frozen_string_literal: true

require "json"

# Snapshot of the FFI signatures attached on `Rdkafka::Bindings`, checked by
# `spec/lib/rdkafka/bindings_parity_spec.rb`. Regenerate the fixture after a deliberate binding
# change:
#
#   bundle exec ruby -Ilib spec/support/bindings_signatures.rb
module BindingsSignatures
  FIXTURE = File.expand_path("../fixtures/bindings_signatures.json", __dir__)

  module_function

  # @return [Hash{String => String}] attached function name => "(params) -> return"
  def dump
    Rdkafka::Bindings
      .attached_functions
      .to_h { |name, function| [name.to_s, signature(function)] }
      .sort
      .to_h
  end

  # @param function [FFI::Function, FFI::VariadicInvoker, FFI::FunctionType]
  # @return [String]
  def signature(function)
    params = function.param_types.map { |type| type_name(type) }.join(", ")
    prefix = function.is_a?(FFI::VariadicInvoker) ? "variadic" : ""

    "#{prefix}(#{params}) -> #{type_name(function.return_type)}"
  end

  # @param type [FFI::Type]
  # @return [String]
  def type_name(type)
    case type
    when FFI::FunctionType
      "callback#{signature(type)}"
    when FFI::Type::Mapped
      converter = type.converter

      case converter
      when FFI::Enum then "enum:#{converter.tag}"
      when FFI::StructByReference then "struct_by_ref:#{converter.struct_class.name.split("::").last}"
      else raise ArgumentError, "Unknown mapped FFI type: #{converter.inspect}"
      end
    when FFI::Type::Builtin
      type.inspect[/Builtin::(\w+)/, 1]
    else
      raise ArgumentError, "Unknown FFI type: #{type.inspect}"
    end
  end
end

if $PROGRAM_NAME == __FILE__
  require "logger"
  require "rdkafka"

  File.write(BindingsSignatures::FIXTURE, "#{JSON.pretty_generate(BindingsSignatures.dump)}\n")
end

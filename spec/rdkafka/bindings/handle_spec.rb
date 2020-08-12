require "spec_helper"

describe Rdkafka::Bindings::Handle do
  let(:native_config) { rdkafka_config.send(:native_config) }

  describe "creating a native handle" do
    subject { Rdkafka::Bindings.new_native_handle(native_config, type) }

    after { subject.close }

    context "for consumer" do
      let(:type) { :consumer }

      it "should return a handle instance" do
        expect(subject).to be_a(described_class)
      end

      it "should set the type to consumer" do
        expect(subject.type).to eq(:consumer)
      end

      it "should have a handle pointer" do
        expect(subject.handle_pointer).to be_a(FFI::Pointer)
      end
    end

    context "for producer" do
      let(:type) { :producer }

      it "should set the type to producer" do
        expect(subject.type).to eq(:producer)
      end

      it "should have a handle pointer" do
        expect(subject.handle_pointer).to be_a(FFI::Pointer)
      end
    end
  end

  describe "creating with invalid type" do
    it "should raise a type error" do
      expect {
        Rdkafka::Bindings.new_native_handle(native_config, nil)
      }.to raise_error(TypeError, "Type has to be a :consumer or :producer")
    end
  end

  describe "closing a native handle" do
    let(:handle) { Rdkafka::Bindings.new_native_handle(native_config, :consumer) }

    it "should set the handle pointer to nil" do
      expect(handle.tap(&:close).handle_pointer).to be_nil
    end

    it "should call the native functions to close" do
      expect(Rdkafka::Bindings).to receive(:close_consumer).and_return(nil)
      expect(Rdkafka::Bindings).to receive(:close_handle).and_return(nil)
      handle.close
    end
  end
end

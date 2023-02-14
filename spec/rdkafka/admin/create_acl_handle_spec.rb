# frozen_string_literal: true

require 'spec_helper'

describe Rdkafka::Admin::CreateACLHandle do
  let(:response) { 0 }

  subject do
    Rdkafka::Admin::CreateACLHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:error_string] = FFI::MemoryPointer::NULL
      handle[:error_code] = 0
    end
  end

  describe '#wait' do
    let(:pending_handle) { true }

    it 'should wait until the timeout and then raise an error' do
      expect do
        subject.wait(max_wait_timeout: 0.1)
      end.to raise_error Rdkafka::Admin::CreateACLHandle::WaitTimeoutError, /create ACL/
    end

    context 'when not pending anymore and no error' do
      let(:pending_handle) { false }

      it 'should return a create ACL report' do
        report = subject.wait

        expect(report.error_string).to eq(nil)
        expect(report.error_code).to eq(nil)
      end

      it 'should wait without a timeout' do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.error_string).to eq(nil)
        expect(report.error_code).to eq(nil)
      end
    end
  end

  describe '#raise_error' do
    let(:pending_handle) { false }

    it 'should raise the appropriate error' do
      expect do
        subject.raise_error
      end.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end

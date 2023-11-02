# frozen_string_literal: true

require "rdkafka/version"

require "rdkafka/helpers/time"
require "rdkafka/abstract_handle"
require "rdkafka/admin"
require "rdkafka/admin/create_topic_handle"
require "rdkafka/admin/create_topic_report"
require "rdkafka/admin/delete_topic_handle"
require "rdkafka/admin/delete_topic_report"
require "rdkafka/bindings"
require "rdkafka/callbacks"
require "rdkafka/config"
require "rdkafka/consumer"
require "rdkafka/consumer/headers"
require "rdkafka/consumer/message"
require "rdkafka/consumer/partition"
require "rdkafka/consumer/topic_partition_list"
require "rdkafka/error"
require "rdkafka/metadata"
require "rdkafka/native_kafka"
require "rdkafka/producer"
require "rdkafka/producer/delivery_handle"
require "rdkafka/producer/delivery_report"

# Main Rdkafka namespace of this gem
module Rdkafka
end

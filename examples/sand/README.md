# Sand Consumer/Producer

## Running the samples

To run these samples, you need an environment that has access to both SAND, and the Kafka
brokers. Any one of the UTL servers will have access.

In the following examples, we are using `devkfk1066.coupadev.com` as the `:client_id`.
For the examples to work, `devkfk1066.coupadev.com` would need the ACLs similar to the
following.

```
Current ACLs for resource `Cluster:LITERAL:kafka-cluster`:
  User:devkfk1066.coupadev.com has Allow permission for operations: IdempotentWrite from hosts: *

Current ACLs for resource `Group:PREFIXED:test`:
  User:devkfk1066.coupadev.com has Allow permission for operations: All from hosts: *

Current ACLs for resource `Topic:PREFIXED:test-topic`:
  User:devkfk1066.coupadev.com has Allow permission for operations: All from hosts: *
```

1. Copy `sand.yml.example` to `sand.yml`, and update `:client_id` and `:client_secret

2. To produce
```
$ bundle exec ./sand_producer.rb -b devkfk1066.coupadev.com -s sand.yml -t test-topic3
Using sand credentials from sand.yml
Producing message to test-topic3
Waiting for delivery
oauthbearer_set_token succeeded
Done
```

3. To consume
```
$ bundle exec ./sand_consumer.rb -b devkfk1066.coupadev.com -s sand.yml -t test-topic3 -g test-topic3
29056
Using sand credentials from sand.yml
Subscribed to test-topic3
oauthbearer_set_token succeeded
Got test-topic3/1@3 Hello Go
Got test-topic3/1@4 Hello Go
Got test-topic3/1@5 Hello Go
Got test-topic3/1@6 Hello Go
Got test-topic3/0@0 Payload from sand_producer
Got test-topic3/0@1 Payload from sand_producer
```

# simple-kafka-client

A Clojure library designed to create and monitor Apache Kafka jobs.

# Usage

## Command line tools

### find-offsets

  $ lein utils find-offsets group-id[ topic]

  e.g.

  $ lein utils find-offsets email-notification-group-id

  $ lein utils find-offsets email-notification-group-id email-notification-topic

### reset-offset

  $ lein utils reset-offset to-offset group-id[ topic comma-seperated-partitions]

  e.g.

  $ lein utils reset-offset 10 email-notification-group-id

  $ lein utils reset-offset 10 email-notification-group-id email-notification-topic

  $ lein utils reset-offset 10 email-notification-group-id email-notification-topic 0,1,2

### Keep in mind

1. Before reset offset, please use find-offsets to observe the current offsets and total size of partitions

2. Then, disable consumer jobs, and do the reset

3. Enable consumer jobs and run find-offsets to verify

# License

Copyright © 2018 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

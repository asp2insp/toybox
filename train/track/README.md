# Track

## Train is a toy implementation of a persistent message queue modeled after Apache Kafka. It should not be used in a production system

The track package represents the persistent file storage of the Train queue. It provides an append-only set of log files which are self-describing. It provides readers that can begin at any offset into the stream.

## TODO
 * Load after restart
 * Garbage Collection
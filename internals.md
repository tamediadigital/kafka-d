# kafka-d internals

## Pipelining and batching

kafka-d supports pipelining and batching of requests through dedicated worker task assigned to each BrokerConnection. When the queue of one of the consumers is not full, the task issues a fetch request in the background. In typical scenario, there will be more than one consumer with empty queue. In this case, the worker task will combine many fetch requests into a single one.

## Consumer queues

Each consumer has an input message queue with configurable size (consumerQueueMaxBytes). Queues improve the throughput by ensuring there is always a message to consume. For example, when consumer pops one message for processing, the next one is fetched in the background (while the consumer processes that message). The background fetches may be batched as described in previous section.

## Producer queues / message batching

kafka-d's producer buffers messages up to configured maximum number of bytes or up to maximum elapsed time. Batch of the messages will be sent to the broker after one of these limits is exceeded.
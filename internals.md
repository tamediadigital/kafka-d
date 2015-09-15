# kafka-d internals

## Overview

### Pipelining and bundling

kafka-d supports pipelining and batching of requests through dedicated fetcher tasks assigned to each BrokerConnection. 
When the queue of one of the consumers is not full, the task issues a fetch request in the background. 
If there are multiple consumers with an empty queue, the fetcher task will combine many fetch requests.

### Consumer queues

Each ```Consumer``` has an ```Queue``` of n (```consumerQueueBuffers```) ```QueueBuffer```'s of configurable size (```consumerMaxBytes``` ). 
Queues improve the throughput by ensuring there is always a message to consume. 
For example, when consumer pops one message for processing, the next one is fetched in the background (while the consumer processes that message). 
The background fetch requests may be bundled as described in previous section.

### Producer queues / message batching

kafka-d's producer buffers messages up to configured maximum number of bytes or up to maximum elapsed time. 
Batch of the messages will be sent to the broker after one of these limits is exceeded.

## How it's implemented

### Queues, queue groups and others

There are few interconnected data structures used within kafkad. 
They are mainly used to implement all the performance features of the kafka network protocol.

One of these features is the request bundling for the fetch and produce requests. 
In each request, the client library may specify multiple topics and partitions. 
Pairs made of topic and partition are called topic partitions. 
The naive way of designing a protocol would be listing those pairs serially, in an array. 
Instead, the kafka designers choose to save some space which in naive approach would be lost due to duplicate data. 
The topic partitions in the protocol are specified like a simple tree. 
First, the topic name is sent, then list of its partitions, then the next topic name, then its partitions, etc.

QueueTopic and QueuePartition classes along with QueueGroup build a dynamic structure which enables fast request bundling.

### Summary of the internal objects

* ```Client``` - acts as a router between broker and consumers and producers. 
It transparently handles the metadata, connection establishment, leader switching, etc.

* ```BrokerConnection``` - handles a single connection to the broker node. 
Each connection holds a group of assigned producers and consumers - or to be more precise - their queues. 
Each consumer's and producer's queue is dynamically assigned to the connection's ```QueueGroup```. 
When the leader for a partition is changed, the consumer's queue may be moved (reassigned) to another connection.

* ```Consumer``` - connects to the client and parses messages from the message sets. 
The message sets are returned from the consumer's queue.

* ```Producer``` - connects to the client, assembles message sets from the messages specified by the user and then pushes them the producer's queue.

* ```Worker``` - a producer or a consumer. It's a general name for both producers and consumers.

* ```Queue``` - both consumers and producers (workers) have queues. Queues belongs to the workers. 
Each worker has exactly one queue. They are used to move filled buffers between the connections and the workers. 
Consumers wait on the queues for the message sets to parse. 
The connection pushes received message sets to the queues. 
Likewisely, producers push prepared message sets to the their queues and connections wait for these message sets. 
When the producer queue has buffers, the connection prepares a request and sends these message sets to the broker.

* ```QueueGroup``` - groups belong to the connections. Each connection has exactly one consumer queue group and one producer queue group.
 Groups hold all consumer and producer queues. When a new consumer or producer is created, its queue is attached to the respecive queue group.

* ```GroupTopic``` and ```GroupPartition``` - they belong to the ```QueueGroup```. 
They are used internally by the ```QueueGroup``` to organize attached consumer and producer queues in a simple tree structure of topics and child partitions. 
They help to quickly search for a topic/partition which is required to handle the response. 
They are also used to build dynamic, bundled requests.

### Data flow

#### Connection
1. A consumer or producer (worker) is attached to the client
2. ```Client``` adds the worker to the internal list of workers
3. ```Client```'s connection manager task, tries to establish the connection to the broker. 
It first looks for respective leader node in the metadata.
4. When the connection is open (either it is already opened or just connected), 
the client attaches worker's queue to the respective queue group of the connection. 
From now, the connection's fetcher task will send fetch requests as long as there are free/unfilled buffers in the consumer queues and the pusher task will send produce requests as long as there are filled buffers in the producer queues. 
The Consumers returns processed buffers to their queues. These buffers become the free/unfilled buffers. 
The producers push the filled buffers to their queues when they finish preparing the batch of messages (a message set).

#### Consumer
1. User calls ```Consumer.getMessage()```
2. If the consumer currenly owns a buffer (message set), it parses the next message and returns it to the user.
3. When the buffer holding the message set becomes empty, the consumer gets the next buffer from its queue. 
Consumer may block, waiting for the buffer, i.e. when its queue is empty and the broker didn't return the data yet.

#### Producer
1. User calls ```Producer.pushMessage(key, value)```
2. If the producer currently owns a buffer for a message set, it assembles the next message and places it in the buffer. 
Otherwise, the producer waits for the next buffer in the queue. 
Buffers are returned to the queue after they are sent to the broker, so the producer may reuse them and prepare subsequent message sets.
3. Producer assembles the message set up to configured timeout (```Config.producerBatchTimeout```) or maximum message set size (```Config.producerMaxBytes```)
4. When the timeout happens or the size limit is reached, the message set is optionally compressed and pushed to the queue.
5. The connection is notified when at least one producer's queue has buffers. 
Then it assembles the produce request, possibly bundling more than one message sets from different producer queues.
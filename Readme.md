Kafka-d is a D Kafka client that depends on the vibe.d framework.

[![Build Status](https://travis-ci.org/tamediadigital/kafka-d.svg?branch=master)](https://travis-ci.org/tamediadigital/kafka-d)

#### Usage
First, bootstrap the client:
```D
Configuration config;
// adjust config's properties if necessary
Client client = new Client([BrokerAddress("localhost", 9092)], "YOUR_CLIENT_ID", config);
```

##### Producing
For a full working example check ```examples/producer/src/app.d```
```D
Producer producer = new Producer(client, topic, partition);
string key = "myKey";
string value = "myValue";
producer.pushMessage(cast(ubyte[])key, cast(ubyte[])value);
```

To avoid double copying of data, you can fill your messages directly to the internal buffer using ```reserveMessage()``` and ```commitMessage()```. The ```pushMessage()``` function uses these two functions internally.

The ```reserveMessage(int keySize, int valueSize)``` function reserves portion of the internal buffer up to specified key and value sizes. Specifying -1 for any of the sizes, makes that field a null value. After the call, the producer.reservedKey and producer.reservedValue can be filled with user data (if the user specified a size greater than 0). The slices have the same sizes as specified in the ```reserveMessage``` call. The messages in the buffer will not be sent until user calls commitMessage(), thus each reserve must be followed by a commit.

The ```commitMessage()``` commits reserved data in the internal buffer. The message becomes queued to be sent to the broker.

```D
Producer producer = new Producer(client, topic, partition);
/// reserve key of size 4 bytes and value of size 8 bytes
producer.reserveMessage(4, 8);
// now the producer.reservedKey and producer.reservedValue are
// the slices pointing to the internal buffer. They are of type ubyte[] and
// their lengths are 4 and 8 respectively.
producer.reservedKey[] = 0; // fill the key with zeros, it uses D slice syntax
producer.reservedValue[] = 0xFF; // fill the value with ubyte.max
// commit the message
producer.commitMessage();

// reserve another message, this time the key is null (the value can be null too)
producer.reserveMessage(-1, 2);
// the reservedKey is not valid (will be sent as null)
// fill two bytes of the value
producer.reservedValue[0] = 0xBE;
producer.reservedValue[1] = 0xEF;
// commit the message
producer.commitMessage();

```

##### Consuming
For each topic and partition, run a worker task:

```D
runWorkerTask((Client client, string topic, int partition) {
    Consumer consumer = new Consumer(client, topic, partition);
    for (;;) {
        Message message = consumer.getMessage();
        // consume the message
    }
}, client, "TOPIC", PARTITION);
```

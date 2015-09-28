Kafka-d is a D Kafka client that depends on the vibe.d framework.

#### Usage
First, bootstrap the client:
```D
Configuration config;
// adjust config's properties if necessary
Client client = new Client([BrokerAddress("localhost", 9092)], "YOUR_CLIENT_ID", config);
while (!client.connect()) {
    // try indefinitely
}
writeln("Connected!");
```

##### Producing
for a full working example check ```examples/producer/src/app.d```
```D
	Producer producer = new Producer(client, topic, partition);
	string key = "myKey";
	string value = "myValue";
	producer.pushMessage(cast(ubyte[])key, cast(ubyte[])value);
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
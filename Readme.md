Kafka-d is a D Kafka client.


##### Working:
* Consumer

##### Not Working:
* Compression
* Producer

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
##### Consuming
For each topic partition, run a worker task:

```D
runWorkerTask((Client client, string topic, int partition) {
    Consumer consumer = new Consumer(client, topic, partition);
    for (;;) {
        Message message = consumer.getMessage();
        // consume the message
    }
}, client, "TOPIC", PARTITION);
```
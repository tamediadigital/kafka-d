import std.stdio : writefln;

import vibe.core.core : runTask, runEventLoop, runWorkerTask;
import vibe.core.log : setLogLevel, LogLevel;

import kafkad.client : BrokerAddress, Client, Configuration, Consumer, Offsets;

void main() {
    runTask({
        debug setLogLevel(LogLevel.debug_);
        
        Configuration config;
        // adjust config's properties if necessary
        
        Client client = new Client([BrokerAddress("127.0.0.1", 9092)], "kafka-d", config);
        
        foreach (topic; client.getTopics()) {
            foreach (partition; client.getPartitions(topic)) {
                writefln("Subscribing topic %s and partition %d", topic, partition);
                runWorkerTask((Client client, string topic, int partition) {
                    Consumer consumer = new Consumer(client, topic, partition, Offsets.Earliest);
                    for (;;) {
                        auto msg = consumer.getMessage();
                        
                        // if the payload consists of UTF-8 characters then it may be safely cast to a string
                        string keyStr = msg.key ? cast(string)msg.key : ""; // msg.key may be null
                        string valueStr = msg.value ? cast(string)msg.value : ""; // msg.value may be null
                        
                        writefln("Topic %s, part. %d, offset %d, key: %s, value: %s",
                                    topic, partition, msg.offset, keyStr, valueStr);
                    }
                }, client, topic, partition);
            }
        }
    });
    runEventLoop();
}

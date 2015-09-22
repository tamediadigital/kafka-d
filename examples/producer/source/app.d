import std.stdio;
import vibe.d;
import kafkad.client;

void main() {
    runTask({
        debug setLogLevel(LogLevel.debug_);
        
        Configuration config;
        // adjust config's properties if necessary
        
        Client client = new Client([BrokerAddress("127.0.0.1", 9092)], "kafka-d", config);
        
        foreach (topic; client.getTopics()) {
            foreach (partition; client.getPartitions(topic)) {
                writefln("Producing for topic %s and partition %d", topic, partition);
                runWorkerTask((Client client, string topic, int partition) {
                    Producer producer = new Producer(client, topic, partition);
                    size_t ctr = 0;
                    import std.format;
                    for (;;) {
                        string key = "myKey";
                        string value = format("myValue%d", ctr++);
                        producer.pushMessage(cast(ubyte[])key, cast(ubyte[])value);
                        sleep(10.msecs);
                    }
                }, client, topic, partition);
            }
        }
    });
    runEventLoop();
}

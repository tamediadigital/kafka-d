import std.stdio;
import vibe.d;
import kafkad.client;

void main() {
    runTask({
        debug setLogLevel(LogLevel.debug_);
        
        Configuration config;
        // adjust config's properties if necessary
        
        Client client = new Client([BrokerAddress("192.168.86.10", 9092)], "kafka-d", config);
        while (!client.connect())
            writeln("Trying to bootstrap kafka client...");
        
        foreach (topic; client.getTopics()) {
            foreach (partition; client.getPartitions(topic)) {
                runWorkerTask((Client client, string topic, int partition) {
                    Consumer consumer = new Consumer(client, topic, partition, StartingOffset.Earliest);
                    for (;;) {
                        Message msg = consumer.getMessage();
                        
                        // if the payload consists of UTF-8 characters then it may be safely cast to a string
                        string keyStr = msg.key ? cast(string)msg.key : ""; // msg.key may be null
                        string valueStr = msg.value ? cast(string)msg.value : ""; // msg.value may be null
                        
                        writefln("Message, offset %d, key: %s, value: %s", msg.offset, keyStr, valueStr);
                    }
                }, client, topic, partition);
            }
        }
    });
    runEventLoop();
}

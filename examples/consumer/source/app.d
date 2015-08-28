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

        //
        foreach (topic; client.getTopics()) {
            foreach (partition; client.getPartitions(topic)) {
                runWorkerTask((Client client, string topic, int partition) {
                    Consumer consumer = new Consumer(client, topic, partition, 0);
                    for (;;) {
                        Message msg = consumer.getMessage();
                        
                        // if the payload consists of UTF-8 characters then it may be safely cast to a string
                        string keyStr = key ? cast(string)key : ""; // key may be null
                        string valueStr = value ? cast(string)value : ""; // value may be null
                        
                        writefln("Message, offset %d, key: %s, value: %s", offset, keyStr, valueStr);
                    }
                }, client, topic, p);
            }
        //

            /+
        auto topics = consumer.consume();

        foreach (ref t; topics) {
            writefln("Topic: %s", t.topic);
            foreach (ref p; t.partitions) {
                writefln("\tPartition: %d, final offset: %d, error: %d", p.partition, p.endOffset, p.errorCode);
                foreach (ref m; p.messages) {
                    writef("\t\tMessage, offset: %d, size: %d: ", m.offset, m.size);
                    foreach (chunk; m.valueChunks) {
                        write(cast(string)chunk);
                    }
                    writeln;
                }
            }
        }+/

    });
    runEventLoop();
}
import std.stdio;
import vibe.d;
import kafkad.client;

void main() {
    runTask({
        debug setLogLevel(LogLevel.debug_);
        auto client = new KafkaClient([BrokerAddress("192.168.86.10", 9092)], "kafka-d");
        while (!client.connect(1))
            writeln("Trying to bootstrap kafka client...");

        auto consumer = new KafkaConsumer(client, [TopicPartitions("kafkad", [PartitionOffset(0, 0)])]);
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
        }

    });
    runEventLoop();
}
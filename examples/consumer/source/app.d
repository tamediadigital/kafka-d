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
            runWorkerTask((Client client) {
                Consumer consumer = new Consumer(client, "kafkad", 0, 0);
                for (;;) {
                    Message msg = consumer.getMessage();
                    long offset = msg.offset;
                    ubyte[] key = msg.key;
                    ubyte[] value = msg.value;
                }
            }, client);
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
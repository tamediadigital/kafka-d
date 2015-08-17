module kafkad.client;

import kafkad.connection;
import kafkad.protocol;
import std.exception;

/*
 * what is needed:
 * - broker list for bootstrap
 * - client id
 */

struct BrokerAddress {
    string host;
    ushort port;
}

class KafkaClient {
    private {
        BrokerConnection m_conns;
        string m_clientId;
    }

    this(BrokerAddress[] bootstrapBrokers, string clientId)
    {
        m_clientId = clientId;
        enforce(bootstrapBrokers.length);
        bootstrap(bootstrapBrokers);
    }

    private void bootstrap(BrokerAddress[] bootstrapBrokers) {
        // to consider: get metadata from one broker or from all and check the consistency between the results
        foreach (brokerAddr; bootstrapBrokers) {
            auto conn = connectBroker(this, brokerAddr);
            auto metadata = conn.getMetadata([]);

            // temporal code, just for first test, metadata will be used mainly internally
            import std.stdio;
            writeln("Broker list:");
            foreach (ref b; metadata.brokers) {
                writefln("\tBroker ID: %d, host: %s, port: %d", b.id, b.host, b.port);
            }
            writeln("Topic list:");
            foreach (ref t; metadata.topics) {
                writefln("\tTopic: %s, partitions:", t.name);
                foreach (ref p; t.partitions) {
                    writefln("\t\tPartition: %d, Leader ID: %d, Replicas: %s, In sync replicas: %s",
                        p.id, p.leader, p.replicas, p.isr);
                }
            }

            auto topics = conn.fetch([TopicPartitions("kafkad", [PartitionOffset(0, 0)])]);
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
        }
    }

    @property auto clientId() { return m_clientId; }
    @property auto clientId(string v) { return m_clientId = v; }
}

enum KafkaCompression {
    None = 0,
    GZIP = 1,
    Snappy = 2
}

class KafkaProducer {
    void put(ubyte[] payload) {
    }
}

struct PartitionOffset {
    int partition;
    long offset;
}

struct TopicPartitions {
    string topic;
    PartitionOffset[] partitions;
}

class KafkaConsumer {
    this(KafkaClient client, TopicPartitions[] topics) {
    }
}
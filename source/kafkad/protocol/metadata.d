module kafkad.protocol.metadata;

import kafkad.protocol.common;
import kafkad.exception;

/* Please note that the types and the order of the fields of these structs must not be changed,
 * as they are used by the deserializer to parse the metadata response */

struct Broker {
    int id;
    string host;
    int port;
}

struct PartitionMetadata {
    ApiError errorCode;
    int id;
    int leader;
    int[] replicas;
    int[] isr;
}

struct TopicMetadata {
    short errorCode;
    string topic;
    PartitionMetadata[] partitions;

    PartitionMetadata findPartitionMetadata(int id) {
        foreach (ref p; partitions) {
            if (p.id == id)
                return p;
        }
        import std.conv;
        throw new MetadataException("Partition " ~ id.to!string ~ " for topic " ~ topic ~ " does not exist");
    }
}

struct Metadata {
    Broker[] brokers;
    TopicMetadata[] topics;

    TopicMetadata findTopicMetadata(string topic) {
        foreach (ref t; topics) {
            if (t.topic == topic)
                return t;
        }
        throw new MetadataException("Topic " ~ topic ~ " does not exist");
    }
}

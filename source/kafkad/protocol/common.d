module kafkad.protocol.common;

/*
 * Kafka requests are always initiated by clients so we only need serializers for requests and deserializers for responses
 * 
 * Kafka 0.8.x network protocol is described here: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
 */

public import core.memory;
public import std.algorithm : min;
public import std.bitmanip;
public import std.exception;
public import std.traits;
public import vibe.core.stream;
public import kafkad.client;

package:

immutable size_t ChunkSize = 4096; // TODO: configurability

enum ApiKey : short {
    ProduceRequest = 0,
    FetchRequest = 1,
    OffsetRequest = 2,
    MetadataRequest = 3,
    //Non-user facing control APIs = 4-7
    OffsetCommitRequest = 8,
    OffsetFetchRequest = 9,
    ConsumerMetadataRequest = 10
}


struct Broker {
    int id;
    string host;
    int port;
}

struct PartitionMetadata {
    short errorCode;
    int id;
    int leader;
    int[] replicas;
    int[] isr;
}

struct TopicMetadata {
    short errorCode;
    string name;
    PartitionMetadata[] partitions;
}

struct MetadataResponse {
    Broker[] brokers;
    TopicMetadata[] topics;
}

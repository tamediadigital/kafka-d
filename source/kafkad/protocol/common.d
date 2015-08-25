module kafkad.protocol.common;

/*
 * Kafka requests are always initiated by clients
 * thus we only need serializers for requests and deserializers for responses
 * 
 * Kafka 0.8.x network protocol is described here: 
* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
 */

public import core.memory;
public import std.algorithm : min;
public import std.bitmanip;
public import std.exception;
public import std.traits;
public import vibe.core.stream;
public import kafkad.client;
import kafkad.exception;

package:

immutable size_t ChunkSize = 4096; // TODO: configurability,document and describe the effects

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

enum ApiError : short {
    /* An unexpected server error */
    Unknown = -1,
    /* No error - it worked! */
    NoError = 0,
    /* The requested offset is outside the range of offsets maintained by the server for the given topic/partition */
    OffsetOutOfRange = 1,
    /* This indicates that a message contents does not match its CRC */
    InvalidMessage = 2,
    /* This request is for a topic or partition that does not exist on this broker */
    UnknownTopicOrPartition = 3,
    /* The message has a negative size */
    InvalidMessageSize = 4,
    /* This error is thrown if we are in the middle of a leadership election and there is currently no leader for
     * this partition and hence it is unavailable for writes */
    LeaderNotAvailable = 5,
    /* This error is thrown if the client attempts to send messages to a replica that is not the leader for some
     * partition. It indicates that the clients metadata is out of date */
    NotLeaderForPartition = 6,
    /* This error is thrown if the request exceeds the user-specified time limit in the request */
    RequestTimedOut = 7,
    /* This is not a client facing error and is used mostly by tools when a broker is not alive */
    BrokerNotAvailable = 8,
    /* If replica is expected on a broker, but is not (this can be safely ignored) */
    ReplicaNotAvailable = 9,
    /* The server has a configurable maximum message size to avoid unbounded memory allocation. This error is
     * thrown if the client attempt to produce a message larger than this maximum */
    MessageSizeTooLarge = 10,
    /* Internal error code for broker-to-broker communication */
    StaleControllerEpochCode = 11,
    /* If you specify a string larger than configured maximum for offset metadata */
    OffsetMetadataTooLargeCode = 12,
    /* The broker returns this error code for an offset fetch request if it is still loading offsets (after a
     * leader change for that offsets topic partition) */
    OffsetsLoadInProgressCode = 14,
    /* The broker returns this error code for consumer metadata requests or offset commit requests if the offsets
     * topic has not yet been created */
    ConsumerCoordinatorNotAvailableCode = 15,
    /* The broker returns this error code if it receives an offset fetch or commit request for a consumer group
     * that it is not a coordinator for */
    NotCoordinatorForConsumerCode = 16
}

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

module kafkad.config;

struct Configuration {
    /// maximum time to wait (msecs) when bundling fetch requests
    /// the longer the waiting, the more fetch requests may be accumulated
    /// the fetcher may bundle faster if there are more requests than fetcherBundleMinRequests
    int fetcherBundleMaxWaitTime = 100;
    /// minimum number of fetch requests to accumulate before bundling them into one request
    /// the fetcher may bundle less requests if the fetcherBundleMaxWaitTime elapses first
    int fetcherBundleMinRequests = 10;
    /// maximum time to wait (msecs) when bundling produce requests
    /// the longer the waiting, the more produce requests may be accumulated
    /// the pusher may bundle faster if there are more requests than pusherBundleMinRequests
    int pusherBundleMaxWaitTime = 100;
    /// minimum number of producer requests to accumulate before bundling them into one request
    /// the pusher may bundle less requests if the pusherBundleMaxWaitTime elapses first
    int pusherBundleMinRequests = 10;
    /// number of retries to perform when waiting for leader election, 0 = retry infinitely
    int leaderElectionRetryCount = 3;
    /// time to wait (msecs) between retries when waiting for leader election
    int leaderElectionRetryTimeout = 1000;
    /// size of the serializer buffer
    int serializerChunkSize = 4096;
    /// size of the deserializer buffer
    int deserializerChunkSize = 4096;
    /// maximum time (msecs) the broker should wait for the receipt of the number of acknowledgements (producerRequiredAcks)
    int produceRequestTimeout = 1000;
    /// this field indicates how many acknowledgements the servers should receive before responding to the request
    /// if it is 0 the server will not send any response (this is the only case where the server will not reply to a request)
    /// if it is 1, the server will wait the data is written to the local log before sending a response
    /// if it is -1 the server will block until the message is committed by all in sync replicas before sending a response
    /// for any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will
    /// never wait for more acknowledgements than there are in-sync replicas)
    short producerRequiredAcks = 1;
    /// maximum time to wait (msecs) for a message set. Batches of messages are prepared up to maximum buffer size or up to
    /// batch timeout, whichever happens first
    int producerBatchTimeout = 100;
    /// maximum number of bytes to include in the message set, this must not be larger than consumerMaxBytes,
    /// otherwise, the consumers may not handle the message sets.
    int producerMaxBytes = 1048576;
    /// number of producer queue buffers, each one has size of producerMaxBytes, must be at least 2
    int producerQueueBuffers = 10;
    /// maximum time to wait (msecs) for messages
    int consumerMaxWaitTime = 100;
    /// minimum number of bytes to accumulate on the server before returning messages
    int consumerMinBytes = 1;
    /// maximum number of bytes to include in the message set
    int consumerMaxBytes = 1048576;
    /// number of consumer queue buffers, each one has size of consumerMaxBytes, must be at least 2
    int consumerQueueBuffers = 10;
    /// number of retries to perform when refreshing the metadata, 0 = retry infinitely
    int metadataRefreshRetryCount = 3;
    /// time to wait (msecs) between retries when refreshing the metadata
    int metadataRefreshRetryTimeout = 1000;
    /// default compression mode for the producers, may be overriden in the Producer constructor
    Compression producerCompression = Compression.None;
    /// valid only for GZIP compression, compression level between 1 and 9: 1 gives best speed, 9 gives best compression
    int producerCompressionLevel = 6;
}

enum Compression {
    Default = -1,
    None = 0,
    GZIP = 1,
    Snappy = 2,
}

int DefaultCompressionLevel = -1;

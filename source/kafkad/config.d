module kafkad.config;

struct Configuration {
    /// maximum length of a topic name in UTF-8 code units
    int maxTopicNameLength = 1024;
    /// number of retries to perform when waiting for leader election, 0 = retry infinitely
    int leaderElectionRetryCount = 3;
    /// time to wait between retries when waiting for leader election
    int leaderElectionRetryTimeout = 1000;
    /// size of the serializer buffer
    int serializerChunkSize = 4096;
    /// size of the deserializer buffer
    int deserializerChunkSize = 4096;
    /// maximum time to wait for messages in ms
    int consumerMaxWaitTime = 100;
    /// minimum number of bytes to accumulate on the server before returning messages
    int consumerMinBytes = 1;
    /// maximum number of bytes to include in the message set
    int consumerMaxBytes = 1048576;
    /// number of queue buffers, each one has size of consumerMaxBytes, must be at least 2
    int consumerQueueBuffers = 10;
    /// number of retries to perform when refreshing the metadata, 0 = retry infinitely
    int metadataRefreshRetryCount = 3;
    /// time to wait between retries when refreshing the metadata
    int metadataRefreshRetryTimeout = 1000;
}
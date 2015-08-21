module kafkad.config;

struct KafkaConfiguration {
    int consumerMaxWaitTime = 100; /// maximum time to wait for messages in ms
    int consumerMinBytes = 1; /// minimum number of bytes to accumulate on the server before returning messages
    int consumerMaxBytes = 1048576; /// maximum number of bytes to include in the message set
}
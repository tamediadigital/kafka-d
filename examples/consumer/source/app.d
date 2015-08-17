import kafkad.client;
import vibe.d;

void main() {
    runTask({
		auto client = new KafkaClient([BrokerAddress("192.168.86.10", 9092)], "kafka-d");
		auto consumer = new KafkaConsumer(client, [TopicPartitions("kafkad", [PartitionOffset(0, 0)])]);

    });
    runEventLoop();
}
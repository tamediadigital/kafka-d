import kafkad.client;
import vibe.d;

void main() {
	runTask({
		auto kc = new KafkaClient([BrokerAddress("192.168.86.10", 9092)], "kafka-d");
		
	});
	runEventLoop();
}
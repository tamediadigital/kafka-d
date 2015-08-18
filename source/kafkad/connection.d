module kafkad.connection;

import kafkad.client;
import kafkad.protocol;
import vibe.core.net;

package:

// todo: leader broker switch when current leader fails

class BrokerConnection {
    private {
        KafkaClient m_client;
        TCPConnection m_conn;
        Serializer m_ser;
        Deserializer m_des;
    }

    int id;

    this(KafkaClient client, TCPConnection conn) {
        m_client = client;
        m_conn = conn;
        m_ser = Serializer(conn);
        m_des = Deserializer(conn);
    }

    // todo: message correlation
    MetadataResponse getMetadata(string[] topics) {
        m_ser.metadataRequest_v0(0, m_client.clientId, topics);
        int size, correlationId;
        m_des.getMessage(size, correlationId);
        assert(correlationId == 0);
        m_des.beginMessage(size);
        return m_des.metadataResponse_v0();
    }

    auto getFetchTopicRange(TopicPartitions[] topics) {
        //Review: correlation id
        m_ser.fetchRequest_v0(0, m_client.clientId, topics);
        int size, correlationId;
        m_des.getMessage(size, correlationId);
        m_des.beginMessage(size);
        return m_des.fetchResponse_v0();
    }
}

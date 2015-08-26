module kafkad.connection;

import kafkad.client;
import kafkad.protocol;
import kafkad.exception;
import vibe.core.core;
import vibe.core.net;
import vibe.core.task;
import vibe.core.sync;

package:

// todo: leader broker switch when current leader fails

struct ConnectionConsumer {
    bool fetchSent;
    ubyte[] msgBuffer;
    string topic;
    int partiton;
    int offset;
}

class BrokerConnection {
    private {
        Client m_client;
        TCPConnection m_conn;
        Serializer m_ser;
        Deserializer m_des;
        int m_correlationId;
        Task m_workerTask;
        Task[int] m_correlations;
        ConnectionConsumer[] m_consumers;
        TopicPartitions[] m_consumerTopics;
        size_t m_consumersReady;
        TaskCondition m_cond;
    }

    void rebuildConsumerTopics() {
        import std.algorithm;
        m_consumers.sort!((a, b) => a.topic < b.topic);
        m_consumerTopics = [];
        foreach (ref c; m_consumers) {
        }
    }

    public void registerConsumer(Consumer consumer) {
    }

    int id = -1;

    @property NetworkAddress addr() {
        return m_conn.remoteAddress.rethrow!ConnectionException("Could not get connection's remote address");
    }

    this(Client client, TCPConnection conn) {
        m_client = client;
        m_conn = conn;
        m_ser = Serializer(conn);
        m_des = Deserializer(conn);
        m_cond = new TaskCondition(new TaskMutex);
        m_workerTask = runTask(&workerMain);
    }

    void workerMain() {
        int size, correlationId;
        for (;;) {
            // send requests
            synchronized (m_cond.mutex) {
                while (!m_consumersReady)
                    m_cond.wait();
                // send
                m_ser.fetchRequest_v0(1, m_client.clientId, m_client.config, m_consumerTopics);
            }

            m_des.getMessage(size, correlationId);
            auto ptask = correlationId in m_correlations;
            if (!ptask)
                break; // TODO: close connection
            m_des.beginMessage(size);
        }
    }

    // todo: message correlation
    Metadata getMetadata(string[] topics) {
        m_ser.metadataRequest_v0(0, m_client.clientId, topics);
        m_ser.flush();
        int size, correlationId;
        m_des.getMessage(size, correlationId);
        assert(correlationId == 0);
        m_des.beginMessage(size);
        return m_des.metadataResponse_v0();
    }

    auto getTopicRange(TopicPartitions[] topics) {
        //Review: correlation id
        m_ser.fetchRequest_v0(0, m_client.clientId, m_client.config, topics);
        int size, correlationId;
        m_des.getMessage(size, correlationId);
        m_des.beginMessage(size);
        return m_des.fetchResponse_v0();
    }

    int fetchRequest(TopicPartitions[] topics) {
        synchronized (this) {
            m_ser.fetchRequest_v0(m_correlationId, m_client.clientId, m_client.config, topics);
            m_ser.flush();
            m_correlations[m_correlationId];
            return m_correlationId++;
        }
    }
}

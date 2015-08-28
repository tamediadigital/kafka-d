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

import std.container.dlist;

enum RequestType { Fetch };

struct Request {
    RequestType type;
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
        QueueGroup m_queueGroup;
        DList!Request m_requests;
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
            synchronized (m_queueGroup.mutex) {
                foreach (ref t; m_queueGroup.queueTopics) {
                }
            }
            synchronized (m_cond.mutex) {
                while (!m_consumersReady)
                    m_cond.wait();
                // send
                //m_ser.fetchRequest_v0(1, m_client.clientId, m_client.config, m_consumerTopics);
            }

            // add request for each fetch, TODO: optimize allocation to the freelist
            auto req = Request(RequestType.Fetch);
            synchronized (this)
                m_requests.insertBack(req);
        }
    }

    void receiverMain() {
        int size, correlationId;
        for (;;) {
            m_des.getMessage(size, correlationId);
            m_des.beginMessage(size);

            // requests are always processed in order on a single TCP connection,
            // and we rely on that order rather than on correlationId
            // requests are pushed to the request queue by the consumer and producer
            // and they are popped here in the order they were sent
            Request req = void;
            synchronized (this) {
                assert(!m_requests.empty);
                req = m_requests.front;
                m_requests.removeFront();
            }

            switch (req.type) {
                case RequestType.Fetch:
                    // parse the fetch response, move returned messages to the correct queues,
                    // and handle partition errors if needed
                    int numtopics;
                    m_des.deserialize(numtopics);
                    assert(numtopics > 0);
                    foreach (nt; 0 .. numtopics) {
                        string topic;
                        int numpartitions;
                        m_des.deserialize(topic); // TODO: preallocate string memory
                        m_des.deserialize(numpartitions);
                        assert(numpartitions > 0);

                        QueueTopic queueTopic = m_queueGroup.findTopic(topic);

                        foreach (np; 0 .. numpartitions) {
                            static struct PartitionInfo {
                                int partition;
                                short errorCode;
                                long endOffset;
                                int messageSetSize;
                            }
                            PartitionInfo pi;
                            m_des.deserialize(pi);
                            // TODO: handle errorCode
                            enforce(pi.messageSetSize <= m_client.config.consumerMaxBytes, "MessageSet is too big to fit into a buffer");

                            // find queue by topic and partition number
                            Queue queue = queueTopic.findQueue(pi.partition);
                            auto qbuf = queue.getBufferToFill();

                            // copy message set to the buffer
                            m_des.deserializeSlice(qbuf.buffer[0 .. pi.messageSetSize]);
                            qbuf.p = qbuf.buffer;
                            qbuf.messageSetSize = pi.messageSetSize;

                            queue.returnFilledBuffer(qbuf);
                        }
                    }
                    break;
                default: assert(0); // FIXME
            }
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

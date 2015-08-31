module kafkad.connection;

import kafkad.client;
import kafkad.protocol;
import kafkad.exception;
import kafkad.consumer.queue;
import kafkad.consumer.group;
import kafkad.utils.lists;
import vibe.core.core;
import vibe.core.net;
import vibe.core.task;
import vibe.core.sync;
import vibe.core.concurrency;

package:

enum RequestType { Metadata, Fetch };

struct Request {
    RequestType type;
    Tid tid;

    Request* next;
}

class BrokerConnection {
    private {
        Client m_client;
        TCPConnection m_conn;
        Serializer m_ser;
        Deserializer m_des;
        TaskMutex m_mutex;
        QueueGroup m_queueGroup;
        FreeList!Request m_requests;
        Task m_fetcherTask, m_receiverTask;
        ubyte[] m_topicNameBuffer;
    }

    int id = -1;

    @property NetworkAddress addr() {
        return m_conn.remoteAddress.rethrow!ConnectionException("Could not get connection's remote address");
    }

    @property QueueGroup queueGroup() {
        return m_queueGroup;
    }

    this(Client client, TCPConnection conn) {
        m_client = client;
        m_conn = conn;
        m_ser = Serializer(conn, client.config.serializerChunkSize);
        m_des = Deserializer(conn, client.config.deserializerChunkSize);
        m_mutex = new TaskMutex();
        m_queueGroup = new QueueGroup();
        m_fetcherTask = runTask(&fetcherMain);
        m_receiverTask = runTask(&receiverMain);
        m_topicNameBuffer = new ubyte[min(client.config.maxTopicNameLength, short.max)];
    }

    void fetcherMain() {
        int size, correlationId;
        for (;;) {
            // send requests
            synchronized (m_queueGroup.mutex) {
                while (!m_queueGroup.fetchRequestTopicsFront)
                    m_queueGroup.freeCondition.wait();

                synchronized (m_mutex) {
                    m_ser.fetchRequest_v0(0, m_client.clientId, m_client.config, m_queueGroup);
                    m_ser.flush();

                    // add request for each fetch
                    auto req = m_requests.getNodeToFill();
                    req.type = RequestType.Fetch;
                    m_requests.pushFilledNode(req);
                }

                m_queueGroup.clearFetchRequestLists();
            }
        }
    }

    void receiverMain() {
        int size, correlationId;
        for (;;) {
            m_des.getMessage(size, correlationId);
            m_des.beginMessage(size);
            scope (success)
                m_des.endMessage();

            // requests are always processed in order on a single TCP connection,
            // and we rely on that order rather than on the correlationId
            // requests are pushed to the request queue by the consumer and producer
            // and they are popped here in the order they were sent
            Request req = void;
            synchronized (m_mutex) {
                assert(!m_requests.empty);
                auto node = m_requests.getNodeToProcess();
                req = *node;
                m_requests.returnProcessedNode(node);
            }

            switch (req.type) {
                case RequestType.Metadata:
                    Metadata metadata = m_des.metadataResponse_v0();
                    send(req.tid, cast(shared)metadata);
                    break;
                case RequestType.Fetch:
                    // parse the fetch response, move returned messages to the correct queues,
                    // and handle partition errors if needed
                    int numtopics;
                    m_des.deserialize(numtopics);
                    assert(numtopics > 0);
                    foreach (nt; 0 .. numtopics) {
                        string topic;
                        int numpartitions;
                        short topicNameLen;
                        m_des.deserialize(topicNameLen);

                        // TODO: send this exception to the consumer and skip the excess bytes
                        enforce(topicNameLen <= m_client.config.maxTopicNameLength, "Topic name is too long");

                        ubyte[] topicSlice = m_topicNameBuffer[0 .. topicNameLen];
                        m_des.deserializeSlice(topicSlice);
                        topic = cast(string)topicSlice;
                        m_des.deserialize(numpartitions);
                        assert(numpartitions > 0);

                        synchronized (m_queueGroup.mutex) {
                            GroupTopic* queueTopic = m_queueGroup.findTopic(topic);

                            foreach (np; 0 .. numpartitions) {
                                static struct PartitionInfo {
                                    int partition;
                                    short errorCode;
                                    long endOffset;
                                    int messageSetSize;
                                }
                                PartitionInfo pi;
                                m_des.deserialize(pi);

                                GroupPartition* queuePartition = null;
                                if (queueTopic)
                                    queuePartition = queueTopic.findPartition(pi.partition);

                                if (!queuePartition) {
                                    // skip the partition
                                    m_des.skipBytes(pi.messageSetSize);
                                    continue;
                                }

                                Queue queue = queuePartition.queue;

                                // TODO: handle errorCode
                                switch (cast(ApiError)pi.errorCode) {
                                    case ApiError.NotLeaderForPartition:
                                        // We need to refresh the metadata, get the new connection and
                                        // retry the request. To do so, we remove the consumer from this
                                        // connection and add it to the client brokerlessConsumers list.
                                        // The client will do the rest.
                                        m_queueGroup.removeQueue(queue);
                                        break;
                                    default: break;
                                }

                                // TODO: send this exception to the consumer and skip the excess bytes
                                enforce(pi.messageSetSize <= m_client.config.consumerMaxBytes, "MessageSet is too big to fit into a buffer");

                                QueueBuffer* qbuf;

                                synchronized (queue.mutex)
                                    qbuf = queue.getBufferToFill();

                                // copy message set to the buffer
                                m_des.deserializeSlice(qbuf.buffer[0 .. pi.messageSetSize]);
                                qbuf.p = qbuf.buffer;
                                qbuf.messageSetSize = pi.messageSetSize;

                                synchronized (queue.mutex) {
                                    queue.returnFilledBuffer(qbuf);
                                    // queue.fetchPending is always true here
                                    if (queue.hasFreeBuffer)
                                        m_queueGroup.queueHasFreeBuffers(queueTopic, queuePartition);
                                    else
                                        queue.fetchPending = false;
                                }
                            }
                        }
                    }
                    break;
                default: assert(0); // FIXME
            }
        }
    }

    Metadata getMetadata(string[] topics) {
        synchronized (m_mutex) {
            m_ser.metadataRequest_v0(0, m_client.clientId, topics);
            m_ser.flush();

            auto req = m_requests.getNodeToFill();
            req.type = RequestType.Metadata;
            req.tid = thisTid;
            m_requests.pushFilledNode(req);
        }
        Metadata ret;
        receive((shared Metadata metadata) { ret = cast()metadata; });
        return ret;
    }
}

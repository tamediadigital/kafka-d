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
import core.time;

package:

enum RequestType { Metadata, Fetch, Offset };

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
        m_topicNameBuffer = new ubyte[short.max];
    }

    void fetcherMain() {
        int size, correlationId;
        bool gotFirstRequest = false;
        MonoTime startTime;
        for (;;) {
            // send requests
            synchronized (m_queueGroup.mutex) {
                if (!gotFirstRequest) {
                    // wait for the first fetch request
                    while (!m_queueGroup.fetchRequestTopicsFront) {
                        m_queueGroup.freeCondition.wait();
                    }
                    if (m_queueGroup.requestsInGroup < m_client.config.fetcherBundleMinRequests) {
                        gotFirstRequest = true;
                        // start the timer
                        startTime = MonoTime.currTime;
                        // wait for more requests
                        continue;
                    }
                } else {
                    // wait up to configured wait time or up to configured request count
                    while (m_queueGroup.requestsInGroup < m_client.config.fetcherBundleMinRequests) {
                        Duration elapsedTime = MonoTime.currTime - startTime;
                        if (elapsedTime >= m_client.config.fetcherBundleMaxWaitTime.msecs)
                            break; // timeout reached
                        Duration remaining = m_client.config.fetcherBundleMaxWaitTime.msecs - elapsedTime;
                        if (!m_queueGroup.freeCondition.wait(remaining))
                            break; // timeout reached
                    }
                    gotFirstRequest = false;
                }

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
                case RequestType.Offset:
                    OffsetResponse_v0 resp = m_des.offsetResponse_v0();
                    send(req.tid, cast(shared)resp);
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
                                    case ApiError.UnknownTopicOrPartition:
                                    case ApiError.LeaderNotAvailable:
                                    case ApiError.NotLeaderForPartition:
                                        // We need to refresh the metadata, get the new connection and
                                        // retry the request. To do so, we remove the consumer from this
                                        // connection and add it to the client brokerlessConsumers list.
                                        // The client will do the rest.
                                        m_queueGroup.removeQueue(queueTopic, queuePartition);
                                        m_des.skipBytes(pi.messageSetSize);
                                        continue;
                                    case ApiError.OffsetOutOfRange:
                                        import std.format;
                                        queue.consumer.throwException(new shared Exception(format(
                                                    "Offset %d is out of range for topic %s, partition %d",
                                                    queue.offset, queueTopic.topic, queuePartition.partition)));
                                        continue;
                                    default: break;
                                }

                                if (pi.messageSetSize > m_client.config.consumerMaxBytes) {
                                    queue.consumer.throwException(new shared ProtocolException("MessageSet is too big to fit into a buffer"));
                                    m_des.skipBytes(pi.messageSetSize);
                                    continue;
                                }

                                QueueBuffer* qbuf;

                                synchronized (queue.mutex)
                                    qbuf = queue.getBufferToFill();

                                // copy message set to the buffer
                                m_des.deserializeSlice(qbuf.buffer[0 .. pi.messageSetSize]);
                                qbuf.p = qbuf.buffer;
                                qbuf.messageSetSize = pi.messageSetSize;

                                // find the next offset to fetch
                                long nextOffset = qbuf.findNextOffset();

                                synchronized (queue.mutex) {
                                    if (nextOffset != -1)
                                        queue.offset = nextOffset;
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
        receive((shared Metadata v) { ret = cast()v; });
        return ret;
    }

    long getStartingOffset(string topic, int partition, long offset) {
        assert(offset == -1 || offset == -2);
        OffsetRequestParams_v0.PartTimeMax[1] p;
        p[0].partition = partition;
        p[0].time = offset;
        p[0].maxOffsets = 1;
        OffsetRequestParams_v0.Topic[1] t;
        t[0].topic = topic;
        t[0].partitions = p;
        OffsetRequestParams_v0 params;
        params.replicaId = id;
        params.topics = t;
        synchronized (m_mutex) {
            m_ser.offsetRequest_v0(0, m_client.clientId, params);
            m_ser.flush();

            auto req = m_requests.getNodeToFill();
            req.type = RequestType.Offset;
            req.tid = thisTid;
            m_requests.pushFilledNode(req);
        }
        shared OffsetResponse_v0 resp;
        receive((shared OffsetResponse_v0 v) { resp = v; });
        enforce(resp.topics.length == 1);
        enforce(resp.topics[0].partitions.length == 1);
        import std.format;
        enforce(resp.topics[0].partitions[0].errorCode == 0,
            format("Could not get starting offset for topic %s and partition %d", topic, partition));
        enforce(resp.topics[0].partitions[0].offsets.length == 1);
        return resp.topics[0].partitions[0].offsets[0];
    }
}

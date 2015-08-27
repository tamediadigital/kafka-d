module kafkad.consumer;

import kafkad.client;
import kafkad.connection;
import kafkad.protocol.fetch;
import vibe.core.sync;
import std.container.dlist;
import std.container.rbtree;
import std.algorithm;

struct Message {
    long offset;
    ubyte[] key;
    ubyte[] value;
}

struct QueueBuffer {
    ubyte[] buffer;
    size_t filled;
}

// queues must be always sorted by the topic
//alias QueueList = RedBlackTree!(Queue, "a.topic < b.topic");

alias QueuePartitions = RedBlackTree!(Queue, "a.partition < b.partition");
alias QueueTopics = RedBlackTree!(QueueTopic, "a.topic < b.topic");

class QueueTopic {
    string topic;
    size_t readyPartitions;
    QueuePartitions queues;

    auto findQueue(int partition) {
        auto r = queues[].find!((a, b) => a.partition == b)(partition);
        assert(!r.empty);
        return r.front;
    }
}

class QueueGroup {
    private {
        QueueTopics m_queueTopics;
        TaskMutex m_mutex;
        TaskCondition m_freeCondition; // notified when there are queues with free buffers
        size_t m_freeQueues;
    }

    this() {
        m_queueTopics = new QueueTopics();
        m_mutex = new TaskMutex();
        m_freeCondition = new TaskCondition(m_mutex);
    }

    @property auto queueTopics() { return m_queueTopics; }
    @property auto mutex() { return m_mutex; }
    @property auto freeCondition() { return m_freeCondition; }

    auto findTopic(string topic) {
        auto r = m_queueTopics[].find!((a, b) => a.topic == b)(topic);
        assert(!r.empty);
        return r.front;
    }

    void notifyQueuesHaveFreeBuffers() {
        m_freeCondition.notify;
    }
}

class Queue {
    private {
        DList!(QueueBuffer*) m_freeBuffers, m_filledBuffers;
        QueueBuffer* m_lastBuffer;
        TaskMutex m_mutex;
        TaskCondition m_filledCondition;
        QueueGroup m_group;
        bool m_fetchPending;
    }

    int partition;

    // this is updated also in the fetch task
    bool fetchPending() { return m_fetchPending; }
    bool fetchPending(bool v) { return m_fetchPending = v; }

    this(in Configuration config, QueueGroup group) {
        import std.algorithm : max;
        auto nbufs = max(2, config.consumerQueueBuffers); // at least 2
        foreach (n; 0 .. nbufs) {
            auto buf = new ubyte[config.consumerMaxBytes];
            auto qbuf = new QueueBuffer(buf, 0);
            m_freeBuffers.insertBack(qbuf);
        }
        m_lastBuffer = null;
        m_mutex = new TaskMutex();
        m_filledCondition = new TaskCondition(m_mutex);
        m_group = group;
        m_fetchPending = false;
    }

    @property auto mutex() { return m_mutex; }
    @property auto filledCondition() { return m_filledCondition; }

    bool hasFreeBuffer() {
        return !m_fetchPending && !m_freeBuffers.empty;
    }

    auto getBufferToFill() {
        auto qbuf = m_freeBuffers.front();
        m_freeBuffers.removeFront();
        return qbuf;
    }

    void returnFilledBuffer(QueueBuffer* buf) {
        synchronized (m_mutex) {
            m_filledBuffers.insertBack(buf);
            m_filledCondition.notify();
        }
    }

    QueueBuffer* waitForFilledBuffer() {
        synchronized (m_mutex) {
            if (m_lastBuffer) {
                // return the last used buffer to the free buffer list
                m_freeBuffers.insertBack(m_lastBuffer);
                // notify the fetch task that there are buffer to be filled in
                // the fetch task will then make a batch request for all queues with free buffers
                // do not notify the task if there is a pending request for this queue (e.g. without a response)
                if (!m_fetchPending)
                    m_group.notifyQueuesHaveFreeBuffers();
            }

            while (m_filledBuffers.empty)
                m_filledCondition.wait();
            m_lastBuffer = m_filledBuffers.front;
            m_filledBuffers.removeFront();
            return m_lastBuffer;
        }
    }

    /*
    void returnProcessedBuffer(QueueBuffer* buf) {
        synchronized (m_mutex) {
            m_freeBuffers.insertBack(buf);
        }
    }*/
}

class Consumer {
    package {
        Client m_client;
        string m_topic;
        int m_partition;
        int m_offset;
        ubyte[] m_msgBuffer;
        size_t m_filled;
        TaskCondition m_cond;
        Queue m_queue;
    }

    package {
        // cached connection to the leader holding selected topic-partition, this is updated on metadata refresh
        BrokerConnection m_conn;
    }

    this(Client client, string topic, int partition, int offset) {
        m_client = client;
        m_topic = topic;
        m_partition = partition;
        m_offset = offset;
        m_queue = new Queue(client.config);
        m_cond = new TaskCondition(new TaskMutex);
    }

    /// Consumes message from the selected topics and partitions
    /// Returns: Ranges of ranges for topics, partitions, messages and message chunks
    /+TopicRange consume() {
        // TEMP HACK
        auto conn = m_client.m_conns.values[0]; // FIXME
        return conn.getTopicRange(m_topics);
    }+/

    Message getMessage() {
        QueueBuffer* qbuf = m_queue.waitForFilledBuffer();
        // TODO: parse qbuf data, check crc, and setup key and value slices FOR EACH MESSAGE, also handle last partial msg
        return Message();
    }
}

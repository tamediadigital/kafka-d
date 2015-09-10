module kafkad.consumer.group;

import kafkad.consumer.queue;
import vibe.core.sync;

struct GroupPartition {
    int partition;
    Queue queue;

    /// field used to build the partition linked list for each topic for the dynamic fetch request
    GroupPartition* next;
}

struct GroupTopic {
    string topic;
    GroupPartition*[int] partitions;
    GroupPartition* fetchRequestPartitionsFront, fetchRequestPartitionsBack;

    // Fields used to build the fetch request. This struct acts both as a holder for a topic and
    // as a linked list node for the fetch request. The request is built dynamically, part by part when a
    // buffer is returned to the free buffer pool (if there is no pending request for this topic/partition).

    /// false if this topic is not in the linked list, true if it is already added
    bool isInFetchRequest;
    /// number of partition in the partition linked list for this topic
    size_t partitionsInFetchRequest;
    /// field used to build the topic linked list for the dynamic fetch request
    GroupTopic* next;

    GroupPartition* findPartition(int partition) {
        auto ppPartition = partition in partitions;
        return ppPartition ? *ppPartition : null;
    }
}

// This represents group of consumer queues for a connection. These queues belong to the consumers
// and are connected to the groups belonging to the connections. Groups build internal data structure
// which allow to perform fast batched fetch requests for multiple topics and partitions.
// Such batching (or bundling) of the requests is very desirable feature, because requests on the
// broker are processed serially, one by one and each fetch request specifies a timeout.
// If many such requests are issued, each one may wait up to config.consumerMaxWaitTime and
// thus, may block the others waiting in the request queue. For example, if maximum wait time
// will me 100 ms, and there will be 10 consumers, then without fetch request bundling, it may
// happen that the last consumer will be waiting for 1 second for the next buffer. In general view,
// many separate fetch requests can significantly reduce performance.
class QueueGroup {
    private {
        GroupTopic*[string] m_groupTopics;
        TaskMutex m_mutex;
        TaskCondition m_freeCondition; // notified when there are queues with free buffers
        size_t m_requestsInGroup;
    }

    GroupTopic* fetchRequestTopicsFront, fetchRequestTopicsBack;

    this() {
        m_mutex = new TaskMutex();
        m_freeCondition = new TaskCondition(m_mutex);
    }

    @property auto groupTopics() { return m_groupTopics; }
    @property auto mutex() { return m_mutex; }
    @property auto freeCondition() { return m_freeCondition; }
    @property auto requestsInGroup() { return m_requestsInGroup; }

    void clearFetchRequestLists() {
        GroupTopic* cur = fetchRequestTopicsFront;
        while (cur) {
            cur.isInFetchRequest = false;
            cur = cur.next;
        }
        fetchRequestTopicsFront = null;
        m_requestsInGroup = 0;
    }

    void queueHasFreeBuffers(Queue queue) {
        auto ptopic = findTopic(queue.consumer.topic);
        assert(ptopic);
        auto ppartition = ptopic.findPartition(queue.consumer.partition);
        assert(ppartition);
        queueHasFreeBuffers(ptopic, ppartition);
    }

    void queueHasFreeBuffers(GroupTopic* ptopic, GroupPartition* ppartition) {
        assert(ptopic);
        assert(ppartition);
        if (!ptopic.isInFetchRequest) {
            if (!fetchRequestTopicsFront) {
                // this will be the first topic in the following fetch request
                fetchRequestTopicsFront = ptopic;
                fetchRequestTopicsBack = ptopic;
            } else {
                assert(fetchRequestTopicsBack);
                fetchRequestTopicsBack.next = ptopic;
                fetchRequestTopicsBack = ptopic;
            }
            ptopic.next = null;
            // this will be the first partition in this topic in the following fetch request
            ptopic.fetchRequestPartitionsFront = ppartition;
            ptopic.fetchRequestPartitionsBack = ppartition;
            ptopic.partitionsInFetchRequest = 1;
            ptopic.isInFetchRequest = true;
            ppartition.next = null;
        } else {
            // the topic is already in the list, so there must be at least one partition
            assert(ptopic.fetchRequestPartitionsBack);
            ptopic.fetchRequestPartitionsBack.next = ppartition;
            ptopic.fetchRequestPartitionsBack = ppartition;
            ++ptopic.partitionsInFetchRequest;
            ppartition.next = null;
        }
        ++m_requestsInGroup;
        m_freeCondition.notify();
    }

    GroupTopic* getOrCreateTopic(string topic) {
        auto ptopic = findTopic(topic);
        if (!ptopic) {
            ptopic = new GroupTopic;
            ptopic.topic = topic;
            m_groupTopics[topic] = ptopic;
        }
        return ptopic;
    }

    void addQueue(Queue queue) {
        synchronized (m_mutex, queue.mutex) {
            auto ptopic = getOrCreateTopic(queue.consumer.topic);
            auto ppartition = new GroupPartition(queue.consumer.partition, queue);
            ptopic.partitions[queue.consumer.partition] = ppartition;

            queue.queueGroup = this;
            if (queue.hasFreeBuffer) {
                queueHasFreeBuffers(ptopic, ppartition);
                queue.fetchPending = true;
            } else {
                queue.fetchPending = false;
            }
        }
    }

    // this is called when m_mutex is already locked
    void removeQueue(GroupTopic* topic, GroupPartition* partition) {
        // remove the partition from the GroupTopic
        assert(topic.partitions.remove(partition.partition));
        if (!topic.partitions.length) {
            // remove the topic from the QueueGroup
            assert(m_groupTopics.remove(topic.topic));
        }
        synchronized (partition.queue.mutex) {
            partition.queue.queueGroup = null;
            partition.queue.fetchPending = false;
        }
    }

    GroupTopic* findTopic(string topic) {
        auto pptopic = topic in m_groupTopics;
        return pptopic ? *pptopic : null;
    }
}

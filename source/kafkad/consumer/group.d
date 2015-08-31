﻿module kafkad.consumer.group;

import kafkad.consumer.queue;
import vibe.core.sync;

struct GroupPartition {
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

class QueueGroup {
    private {
        GroupTopic*[string] m_groupTopics;
        TaskMutex m_mutex;
        TaskCondition m_freeCondition; // notified when there are queues with free buffers
    }

    GroupTopic* fetchRequestTopicsFront, fetchRequestTopicsBack;

    this() {
        m_mutex = new TaskMutex();
        m_freeCondition = new TaskCondition(m_mutex);
    }

    @property auto groupTopics() { return m_groupTopics; }
    @property auto mutex() { return m_mutex; }
    @property auto freeCondition() { return m_freeCondition; }

    void clearFetchRequestLists() {
        GroupTopic* cur = fetchRequestTopicsFront;
        while (cur) {
            cur.isInFetchRequest = false;
            cur = cur.next;
        }
        fetchRequestTopicsFront = null;
    }

    void queueHasFreeBuffers(Queue queue) {
        auto ptopic = findTopic(queue.consumer.topic);
        assert(ptopic);
        auto ppartition = ptopic.findPartition(queue.consumer.partition);
        assert(ppartition);
        queueHasFreeBuffers(ptopic, ppartition);
    }

    void queueHasFreeBuffers(GroupTopic* ptopic, GroupPartition* ppartition) {
        if (!ptopic.isInFetchRequest) {
            if (!fetchRequestTopicsFront) {
                // this will be the first topic in the following fetch request
                fetchRequestTopicsFront = ptopic;
                fetchRequestTopicsBack = ptopic;
                ptopic.next = null;
            } else {
                assert(fetchRequestTopicsBack);
                fetchRequestTopicsBack.next = ptopic;
                fetchRequestTopicsBack = ptopic;
                ptopic = null;
            }
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
            auto ppartition = new GroupPartition(queue);
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

    void removeQueue(Queue queue) {
        //fixme
    }

    GroupTopic* findTopic(string topic) {
        auto pptopic = topic in m_groupTopics;
        return pptopic ? *pptopic : null;
    }
}
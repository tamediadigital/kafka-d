module kafkad.bundler;

import core.sync.mutex;
import kafkad.queue;
import vibe.core.sync;

struct Partition {
    int partition;
    Queue queue;
    QueueBuffer* buffer;

    /// field used to build the partition linked list for each topic for the dynamic fetch/produce request
    Partition* next;
}

struct Topic {
    string topic;
    Partition*[int] partitions;
    Partition* requestPartitionsFront, requestPartitionsBack;

    // Fields used to build the fetch/produce request. This struct acts both as a holder for a topic and
    // as a linked list node for the request. The request is built dynamically, part by part when a
    // buffer becomes ready in the queue (if there is no pending request for this topic/partition).
    // Buffer readiness for the consumer is when there are free buffers to be filled up, and for the
    // producer when there are filled buffers to be pushed to the cluster.

    /// false if this topic is not in the linked list, true if it is already added
    bool isInRequest;
    /// number of partition in the partition linked list for this topic
    size_t partitionsInRequest;
    /// field used to build the topic linked list for the dynamic fetch/produce request
    Topic* next;

    Partition* findPartition(int partition) {
        auto ppPartition = partition in partitions;
        return ppPartition ? *ppPartition : null;
    }
}

// Request bundler build internal data structure which allow to perform fast batched fetch/produce
// requests for multiple topics and partitions. Such batching (or bundling) of the requests is very
// desirable feature, because requests on the broker are processed serially, one by one and each
// fetch/produce request specifies a timeout. If many such requests are issued, each one may wait
// up to config.consumerMaxWaitTime/produceRequestTimeout and thus, may block the others waiting
// in the request queue. For example, if maximum wait time will me 100 ms, and there will be 10
// consumers, then without the request bundling, it may happen that the last consumer will be
// waiting for 1 second for the next buffer. In general view, many separate fetch/produce requests
// can significantly reduce performance.
class RequestBundler {
    private {
        Topic*[string] m_topics;
        TaskMutex m_mutex;
        InterruptibleTaskCondition m_readyCondition; // notified when there are queues with ready buffers
        size_t m_queueCount;
        size_t m_requestsCollected;
    }

    Topic* requestTopicsFront, requestTopicsBack;

    this() {
        m_mutex = new TaskMutex();
        m_readyCondition = new InterruptibleTaskCondition(cast(core.sync.mutex.Mutex)m_mutex);
    }

    @property auto topics() { return m_topics; }
    @property auto mutex() { return m_mutex; }
    @property auto readyCondition() { return m_readyCondition; }
    @property auto queueCount() { return m_queueCount; }
    @property auto requestsCollected() { return m_requestsCollected; }

    void clearRequestLists() {
        Topic* cur = requestTopicsFront;
        while (cur) {
            cur.isInRequest = false;
            cur = cur.next;
        }
        requestTopicsFront = null;
        m_queueCount = 0;
        m_requestsCollected = 0;
    }

    void queueHasReadyBuffers(Queue queue) {
        auto ptopic = findTopic(queue.worker.topic);
        assert(ptopic);
        auto ppartition = ptopic.findPartition(queue.worker.partition);
        assert(ppartition);
        queueHasReadyBuffers(ptopic, ppartition);
    }

    void queueHasReadyBuffers(Topic* ptopic, Partition* ppartition) {
        assert(ptopic);
        assert(ppartition);
        if (!ptopic.isInRequest) {
            if (!requestTopicsFront) {
                // this will be the first topic in the following request
                requestTopicsFront = ptopic;
                requestTopicsBack = ptopic;
            } else {
                assert(requestTopicsBack);
                requestTopicsBack.next = ptopic;
                requestTopicsBack = ptopic;
            }
            ptopic.next = null;
            // this will be the first partition in this topic in the following request
            ptopic.requestPartitionsFront = ppartition;
            ptopic.requestPartitionsBack = ppartition;
            ptopic.partitionsInRequest = 1;
            ptopic.isInRequest = true;
            ppartition.next = null;
        } else {
            // the topic is already in the list, so there must be at least one partition
            assert(ptopic.requestPartitionsBack);
            ptopic.requestPartitionsBack.next = ppartition;
            ptopic.requestPartitionsBack = ppartition;
            ++ptopic.partitionsInRequest;
            ppartition.next = null;
        }
        ++m_requestsCollected;
        m_readyCondition.notify();
    }

    Topic* getOrCreateTopic(string topic) {
        auto ptopic = findTopic(topic);
        if (!ptopic) {
            ptopic = new Topic;
            ptopic.topic = topic;
            m_topics[topic] = ptopic;
        }
        return ptopic;
    }

    void addQueue(Queue queue, BufferType readyBufferType) {
        synchronized (m_mutex) {
			synchronized(queue.mutex) {
            	auto ptopic = getOrCreateTopic(queue.worker.topic);
            	auto ppartition = new Partition(queue.worker.partition, queue);
            	ptopic.partitions[queue.worker.partition] = ppartition;

            	queue.requestBundler = this;
            	if (queue.hasBuffer(readyBufferType)) {
            	    queueHasReadyBuffers(ptopic, ppartition);
            	    queue.requestPending = true;
            	} else {
            	    queue.requestPending = false;
            	}

            	BufferType workerBufferType = readyBufferType == BufferType.Free ? BufferType.Filled : BufferType.Free;
            	if (queue.hasBuffer(workerBufferType))
            	    queue.condition.notify();
            	++m_queueCount;
			}
        }
    }

    // this is called when m_mutex is already locked
    void removeQueue(Topic* topic, Partition* partition) {
        // remove the partition from the Topic
        assert(topic.partitions.remove(partition.partition));
        if (!topic.partitions.length) {
            // remove the topic from the RequestBundler
            assert(m_topics.remove(topic.topic));
        }
        synchronized (partition.queue.mutex) {
            partition.queue.requestBundler = null;
            partition.queue.requestPending = false;
        }
        --m_queueCount;
    }

    Topic* findTopic(string topic) {
        auto pptopic = topic in m_topics;
        return pptopic ? *pptopic : null;
    }

    int queues(int delegate(Queue) dg) {
        foreach (t; m_topics.byValue) {
            foreach (p; t.partitions.byValue) {
                auto ret = dg(p.queue);
                if (ret)
                    return ret;
            }
        }
        return 0;
    }

}

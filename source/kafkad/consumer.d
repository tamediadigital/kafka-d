module kafkad.consumer;

import kafkad.client;
import kafkad.protocol.fetch;

struct Message {
    long offset;
    ubyte[] key;
    ubyte[] message;
}

class Consumer {
    private {
        Client m_client;
        string m_topic;
        int m_partition;
        int m_offset;
    }

    this(Client client, string topic, int partition, int offset) {
        m_client = client;
        m_topic = topic;
        m_partition = partition;
        m_offset = offset;
    }

    /// Consumes message from the selected topics and partitions
    /// Returns: Ranges of ranges for topics, partitions, messages and message chunks
    TopicRange consume() {
        // TEMP HACK
        auto conn = m_client.m_conns.values[0]; // FIXME
        return conn.getTopicRange(m_topics);
    }

    Message getMessage() {
        return Message();
    }
}

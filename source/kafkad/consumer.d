module kafkad.consumer;

import kafkad.client;
import kafkad.protocol.fetch;

class Consumer {
    private {
        Client m_client;
        TopicPartitions[] m_topics;
    }
    this(Client client, TopicPartitions[] topics) {
        m_client = client;
        m_topics = topics;
    }

    /// Consumes message from the selected topics and partitions
    /// Returns: Ranges of ranges for topics, partitions, messages and message chunks
    TopicRange consume() {
        // TEMP HACK
        auto conn = m_client.m_conns.values[0]; // FIXME
        return conn.getTopicRange(m_topics);
    }
}

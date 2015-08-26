module kafkad.consumer;

import kafkad.client;
import kafkad.connection;
import kafkad.protocol.fetch;

struct Message {
    long offset;
    ubyte[] key;
    ubyte[] value;
}

class Consumer {
    package {
        Client m_client;
        string m_topic;
        int m_partition;
        int m_offset;
        ubyte[] m_msgBuffer;
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
        //m_msgBuffer = 
    }

    /// Consumes message from the selected topics and partitions
    /// Returns: Ranges of ranges for topics, partitions, messages and message chunks
    /+TopicRange consume() {
        // TEMP HACK
        auto conn = m_client.m_conns.values[0]; // FIXME
        return conn.getTopicRange(m_topics);
    }+/

    Message getMessage() {
        //m_conn.
        return Message();
    }
}

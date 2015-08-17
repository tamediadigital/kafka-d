module kafkad.protocol.fetch;

import kafkad.protocol.common;
import kafkad.protocol.deserializer;

struct MessageChunkRange {
    private {
        size_t m_remaining, m_chunkSize;
        ubyte[] m_front;
        Deserializer* m_des;
    }

    private this(Deserializer* des, size_t size) {
        m_remaining = size;
        m_chunkSize = 0;
        m_des = des;
        if (size)
            popFront();
    }

    @property bool empty() {
        return !m_remaining;
    }
    @property ubyte[] front() {
        return m_front;
    }
    void popFront() {
        enforce(!empty);
        m_remaining -= m_chunkSize;
        if (!m_remaining)
            return;
        m_front = m_des.getChunk(m_remaining);
        m_chunkSize = m_front.length;
    }
}

struct FetchMessage {
    long offset;
    int size;
    MessageChunkRange valueChunks;
}

// NOTE: message sets aren't prefixed by array length
struct FetchMessageRange {
    private {
        size_t m_remaining, m_structSize;
        FetchMessage m_front;
        Deserializer* m_des;
    }

    private this(Deserializer* des, size_t messageSetSize) {
        m_remaining = messageSetSize;
        m_structSize = 0;
        m_des = des;
        if (messageSetSize)
            popFront();
    }

    @property bool empty() {
        return !m_remaining;
    }
    @property auto ref front() {
        return m_front;
    }
    void popFront() {
        enforce(!empty);
        m_remaining -= m_structSize;
        if (!m_remaining)
            return;
        m_des.deserialize(m_front.offset);
        int structSize;
        m_des.deserialize(structSize); // Message struct size including crc, attributes, etc.
        m_structSize = 8 + 4 + structSize;
        int crc;
        byte magicByte, attributes;
        m_des.deserialize(crc);
        m_des.deserialize(magicByte);
        m_des.deserialize(attributes);
        if ((attributes & 3) != 0)
            throw new Exception("Message compression is not yet supported");
        int size;
        m_des.deserialize(size); // key length
        if (size > 0) // size == -1 means key is null
            m_des.skipBytes(size); // we don't support keys yet
        m_des.deserialize(m_front.size); // value length
        m_front.valueChunks = MessageChunkRange(m_des, m_front.size);
    }
}

struct FetchPartition {
    int partition;
    short errorCode;
    long endOffset;
    FetchMessageRange messages;
}

struct FetchPartitionRange {
    private {
        size_t m_iter, m_length;
        FetchPartition m_front;
        Deserializer* m_des;
    }

    private this(Deserializer* des, size_t numpartitions) {
        m_iter = 0;
        m_des = des;
        m_length = numpartitions;
        if (m_length)
            popFront(true);
    }

    @property size_t length() {
        return m_length;
    }
    @property bool empty() {
        assert(m_iter <= m_length);
        return m_iter == m_length;
    }
    @property auto ref front() {
        return m_front;
    }
    void popFront(bool first = false) {
        enforce(!empty);
        if (!first) {
            ++m_iter;
            if (empty)
                return;
        }
        m_des.deserialize(m_front.partition);
        m_des.deserialize(m_front.errorCode);
        m_des.deserialize(m_front.endOffset);
        int messageSetSize;
        m_des.deserialize(messageSetSize);
        m_front.messages = FetchMessageRange(m_des, messageSetSize);
    }
}

struct FetchTopic {
    string topic; // TODO: cache string memory, to prevent allocation in the deserializer
    FetchPartitionRange partitions;
}

struct FetchTopicRange {
    private {
        size_t m_iter, m_length;
        FetchTopic m_front;
        Deserializer* m_des;
    }

    package this(Deserializer* des, size_t numtopics) {
        m_iter = 0;
        m_des = des;
        m_length = numtopics;
        if (m_length)
            popFront(true);
    }

    @property size_t length() {
        return m_length;
    }
    @property bool empty() {
        assert(m_iter <= m_length);
        return m_iter == m_length;
    }
    @property auto ref front() {
        return m_front;
    }
    void popFront(bool first = false) {
        enforce(!empty);
        if (!first) {
            ++m_iter;
            if (empty)
                return;
        }
        m_des.deserialize(m_front.topic);
        int numpartitions;
        m_des.deserialize(numpartitions);
        m_front.partitions = FetchPartitionRange(m_des, numpartitions);
    }
}

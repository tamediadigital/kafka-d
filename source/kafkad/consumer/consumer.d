module kafkad.consumer.consumer;

import kafkad.client;
import kafkad.exception;
import kafkad.consumer.queue;
import std.exception;
import core.atomic;

alias Offset = long;
enum Offsets : Offset { Latest = -1, Earliest = -2 }

/// Message returned by the consumer
struct Message {
    /// Message offset in the log
    long offset;
    /// The key of the message, may be null
    ubyte[] key;
    /// The value of the message, may be null
    ubyte[] value;
}

class Consumer {
    private {
        Client m_client;
        string m_topic;
        int m_partition;
        Queue m_queue;
        QueueBuffer* m_currentBuffer;
    }

    package (kafkad) {
        @property queue() { return m_queue; }
        /// Throws an exception in the consumer task. This is used to pass the connection exceptions to the user.
        void throwException(Exception ex) {
            synchronized (m_queue.mutex)
                m_queue.appendExceptionBuffer(ex);
        }
    }

    @property auto topic() { return m_topic; }
    @property auto partition() { return m_partition; }

    this(Client client, string topic, int partition, Offset startingOffset) {
        enforce(startingOffset >= -2);
        m_client = client;
        m_topic = topic;
        m_partition = partition;
        m_queue = new Queue(this, client.config);
        m_queue.offset = startingOffset;
        m_currentBuffer = null;

        client.addNewConsumer(this);
    }

    Message getMessage() {
        if (!m_currentBuffer)
            m_currentBuffer = m_queue.waitForFilledBuffer();
    processBuffer:
        if (m_currentBuffer.messageSetSize > 12 /* Offset + Message Size */) {
            import std.bitmanip, std.digest.crc;

            long offset = bigEndianToNative!long(m_currentBuffer.p[0 .. 8]);
            int messageSize = bigEndianToNative!int(m_currentBuffer.p[8 .. 12]);
            m_currentBuffer.p += 12;
            m_currentBuffer.messageSetSize -= 12;
            if (m_currentBuffer.messageSetSize >= messageSize) {
                // we got full message here
                scope (exit) {
                    m_currentBuffer.p += messageSize;
                    m_currentBuffer.messageSetSize -= messageSize;
                }
                uint messageCrc = bigEndianToNative!uint(m_currentBuffer.p[0 .. 4]);
                // check remainder bytes with CRC32 and compare
                ubyte[4] computedCrc = crc32Of(m_currentBuffer.p[4 .. messageSize]);
                if (*cast(uint*)&computedCrc != messageCrc) {
                    // handle CRC error
                    throw new CrcException("Invalid message checksum");
                }
                byte magicByte = m_currentBuffer.p[4];
                enforce(magicByte == 0);
                byte attributes = m_currentBuffer.p[5];
                int keyLen = bigEndianToNative!int(m_currentBuffer.p[6 .. 10]);
                ubyte[] key = null;
                if (keyLen >= 0) {
                    // 14 = crc(4) + magicByte(1) + attributes(1) + keyLen(4) + valueLen(4)
                    enforce(keyLen <= messageSize - 14);
                    key = m_currentBuffer.p[10 .. 10 + keyLen];
                }
                auto pValue = m_currentBuffer.p + 10 + key.length;
                int valueLen = bigEndianToNative!int(pValue[0 .. 4]);
                ubyte[] value = null;
                if (valueLen >= 0) {
                    enforce(valueLen <= messageSize - 14 - key.length);
                    pValue += 4;
                    value = pValue[0 .. valueLen];
                }

                byte compression = attributes & 3;
                if (compression != 0) {
                    // handle compression, this must be the only message in a message set
                    // TODO: In general, the return messages will have offsets larger than or equal
                    // to the starting offset. However, with compressed messages, it's possible for the returned
                    // messages to have offsets smaller than the starting offset. The number of such messages is
                    // typically small and the caller is responsible for filtering out those messages.
                    assert(0); // FIXME
                } else {
                    // no compression, just return the message
                    return Message(offset, key, value);
                }
            } else {
                // this is the last, partial message, skip it
                m_currentBuffer = m_queue.waitForFilledBuffer();
                goto processBuffer;
            }
        } else {
            // no more messages, get next buffer
            m_currentBuffer = m_queue.waitForFilledBuffer();
            goto processBuffer;
        }
    }
}

module kafkad.consumer;

import kafkad.client;
import kafkad.exception;
import kafkad.queue;
import kafkad.worker;
import kafkad.utils.snappy;
import etc.c.zlib;
import std.algorithm : swap;
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

class Consumer : IWorker {
    private {
        Client m_client;
        string m_topic;
        int m_partition;
        Queue m_queue;
        QueueBuffer* m_currentBuffer, m_compressionBuffer;
        bool m_isDecompressedBuffer;
        z_stream m_zlibContext;
        Message m_message;
    }

    package (kafkad) {
        @property queue() { return m_queue; }
    }

    /// Throws an exception in the consumer task. This is used to pass the connection exceptions to the user.
    void throwException(Exception ex) {
        m_queue.exception = ex;
        m_queue.condition.notify();
    }

    @property string topic() { return m_topic; }
    @property int partition() { return m_partition; }

    @property WorkerType workerType() { return WorkerType.Consumer; }

    this(Client client, string topic, int partition, Offset startingOffset) {
        enforce(startingOffset >= -2);
        m_client = client;
        m_topic = topic;
        m_partition = partition;
        m_queue = new Queue(this, m_client.config.consumerQueueBuffers, m_client.config.consumerMaxBytes);
        m_queue.offset = startingOffset;
        m_currentBuffer = null;
        m_compressionBuffer = new QueueBuffer(m_client.config.consumerMaxBytes);
        m_isDecompressedBuffer = false;

        m_zlibContext.next_in = null;
        m_zlibContext.avail_in = 0;
        m_zlibContext.zalloc = null;
        m_zlibContext.zfree = null;
        m_zlibContext.opaque = null;
        enforce(inflateInit2(&m_zlibContext, 15 + 32) == Z_OK);
        scope (failure)
            inflateEnd(&m_zlibContext);

        client.addNewConsumer(this);
    }

    private void swapBuffers(bool isDecompressedBuffer) {
        swap(m_currentBuffer, m_compressionBuffer);
        m_isDecompressedBuffer = isDecompressedBuffer;
    }
    
    Message front()
    {
        return m_message;
    }
    
    void popFront()
    {
        m_message = getMessage();
    }
    
    bool empty()
    { 
        return false;
    }

    int opApply(int delegate( Message) dg)
    {
       int result;
       for(;;)
       {
         popFront();
         if (dg(front())) 
            return result;         
       }
       assert(0);
    }
    
    // TODO: make private
    Message getMessage() {
        if (!m_currentBuffer) {
            synchronized (m_queue.mutex) {
                m_currentBuffer = m_queue.waitForBuffer(BufferType.Filled);
            }
        }
    processBuffer:
        if (m_currentBuffer.messageSetSize > 12 /* Offset + Message Size */) {
            import std.bitmanip, std.digest.crc;

            long offset = bigEndianToNative!long(m_currentBuffer.p[0 .. 8]);
            int messageSize = bigEndianToNative!int(m_currentBuffer.p[8 .. 12]);
            m_currentBuffer.p += 12;
            m_currentBuffer.messageSetSize -= 12;
            if (m_currentBuffer.messageSetSize >= messageSize) {
                // we got full message here
                void skipMessage() {
                    m_currentBuffer.p += messageSize;
                    m_currentBuffer.messageSetSize -= messageSize;
                }
                if (offset < m_currentBuffer.requestedOffset) {
                    // In general, the return messages will have offsets larger than or equal
                    // to the starting offset. However, with compressed messages, it's possible for the returned
                    // messages to have offsets smaller than the starting offset. The number of such messages is
                    // typically small and the caller is responsible for filtering out those messages.
                    skipMessage();
                    goto processBuffer;
                }
                uint messageCrc = bigEndianToNative!uint(m_currentBuffer.p[0 .. 4]);
                // check remainder bytes with CRC32 and compare
                ubyte[4] computedCrc = crc32Of(m_currentBuffer.p[4 .. messageSize]);
                if (*cast(uint*)&computedCrc != messageCrc) {
                    // handle CRC error
                    skipMessage();
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
                    enforce(!m_isDecompressedBuffer, new ProtocolException("Recursive compression is not supported"));
                    enforce(value.length);
                    switch (compression) {
                        case Compression.GZIP:
                            inflateReset(&m_zlibContext);
                            m_zlibContext.next_in = value.ptr;
                            m_zlibContext.avail_in = cast(uint)value.length;
                            m_zlibContext.next_out = m_compressionBuffer.buffer;
                            m_zlibContext.avail_out = m_client.config.consumerMaxBytes;
                            int r = inflate(&m_zlibContext, Z_FINISH);
                            if (r == Z_STREAM_END) {
                                m_compressionBuffer.rewind();
                                m_compressionBuffer.messageSetSize = m_zlibContext.total_out;
                            } else {
                                if (m_zlibContext.avail_out)
                                    throw new ProtocolException("GZIP decompressed message set is too big to fit into the buffer");
                                else
                                    throw new ProtocolException("GZIP could not decompress the message set");
                            }
                            break;
                        case Compression.Snappy:
                            size_t outLen = m_client.config.consumerMaxBytes;
                            int r = snappy_java_uncompress(value.ptr, value.length, m_compressionBuffer.buffer, &outLen);
                            if (r == SNAPPY_OK) {
                                m_compressionBuffer.rewind();
                                m_compressionBuffer.messageSetSize = outLen;
                            } else {
                                if (r == SNAPPY_BUFFER_TOO_SMALL)
                                    throw new ProtocolException("Snappy decompressed message set is too big to fit into the buffer");
                                else
                                    throw new ProtocolException("Snappy could not decompress the message set");
                            }
                            break;
                        default: throw new ProtocolException("Unsupported compression type");
                    }

                    m_compressionBuffer.requestedOffset = m_currentBuffer.requestedOffset;
                    skipMessage();
                    swapBuffers(true);
                    goto processBuffer;
                } else {
                    // no compression, just return the message
                    skipMessage();
                    return Message(offset, key, value);
                }
            } else {
                // this is the last, partial message, skip it
                if (m_isDecompressedBuffer) {
                    swapBuffers(false);
                    goto processBuffer;
                }
                synchronized (m_queue.mutex) {
                    m_queue.returnBuffer(BufferType.Free, m_currentBuffer);
                    m_queue.notifyRequestBundler();
                    m_currentBuffer = m_queue.waitForBuffer(BufferType.Filled);
                }
                goto processBuffer;
            }
        } else {
            // no more messages, get next buffer
            if (m_isDecompressedBuffer) {
                swapBuffers(false);
                goto processBuffer;
            }
            synchronized (m_queue.mutex) {
                m_queue.returnBuffer(BufferType.Free, m_currentBuffer);
                m_queue.notifyRequestBundler();
                m_currentBuffer = m_queue.waitForBuffer(BufferType.Filled);
            }
            goto processBuffer;
        }
    }
}

module kafkad.producer;

import kafkad.client;
import kafkad.exception;
import kafkad.queue;
import kafkad.worker;
import kafkad.utils.snappy;
import etc.c.zlib;
import std.exception;
import core.time;
import vibe.core.core;
import vibe.core.sync;

class Producer : IWorker {
    enum __isWeakIsolatedType = true; // needed to pass this type between vibe.d's tasks
    private {
        Client m_client;
        string m_topic;
        int m_partition;
        Queue m_queue;
        QueueBuffer* m_currentBuffer;
        TaskCondition m_batchCondition;
        bool m_batchStarted, m_isFirstMessage, m_bufferReserved;
        MonoTime m_batchDeadline;
        Compression m_compression;
        QueueBuffer* m_compressionBuffer;
        z_streamp m_zlibContext;

        int m_messageSize;
        int m_keySize, m_valueSize;
        ubyte[] m_reservedKey, m_reservedValue;
    }

    package (kafkad) {
        @property queue() { return m_queue; }
    }

    /// Throws an exception in the producer task. This is used to pass the connection exceptions to the user.
    void throwException(Exception ex) {
        m_queue.exception = ex;
        m_queue.condition.notify();
    }

    @property string topic() { return m_topic; }
    @property int partition() { return m_partition; }

    @property WorkerType workerType() { return WorkerType.Producer; }

    /// Slice from the internal buffer for the key. Note that reserveMessage() must be called first
    @property ubyte[] reservedKey() { assert(m_bufferReserved); return m_reservedKey; }
    /// Slice from the internal buffer for the value. Note that reserveMessage() must be called first
    @property ubyte[] reservedValue() { assert(m_bufferReserved); return m_reservedValue; }

    /// Params:
    ///     client = the client instance
    ///     topic = producer topic
    ///     partition = producer partition
    ///     compression = the compression type, it uses config.producerCompression by default
    ///     compressionLevel = valid only for GZIP compression, compression level between
    ///                        1 and 9: 1 gives best speed, 9 gives best compression, it uses
    ///                        config.producerCompressionLevel by default
    this(Client client, string topic, int partition, Compression compression, int compressionLevel) {
        m_client = client;
        m_topic = topic;
        m_partition = partition;
        m_queue = new Queue(this, m_client.config.producerQueueBuffers, m_client.config.producerMaxBytes);
        m_currentBuffer = null;
        m_batchCondition = new TaskCondition(m_queue.mutex);
        m_batchStarted = false;
        m_compression = compression;
        m_compressionBuffer = m_compression != Compression.None ? new QueueBuffer(m_client.config.producerMaxBytes) : null;
        if (m_compression == Compression.GZIP) {
            m_zlibContext = new z_stream;
            m_zlibContext.zalloc = null;
            m_zlibContext.zfree = null;
            m_zlibContext.opaque = null;
            enforce(deflateInit2(m_zlibContext, compressionLevel, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) == Z_OK);
        }
        scope (failure) if (m_compression == Compression.GZIP)
            deflateEnd(m_zlibContext);

        client.addNewProducer(this);
        runTask(&batcherMain);
    }

    /// Params:
    ///     client = the client instance
    ///     topic = producer topic
    ///     partition = producer partition
    ///     compression = the compression type, it uses config.producerCompression by default
    this(Client client, string topic, int partition, Compression compression) {
        this(client, topic, partition, compression, client.config.producerCompressionLevel);
    }

    /// Params:
    ///     client = the client instance
    ///     topic = producer topic
    ///     partition = producer partition
    this(Client client, string topic, int partition) {
        this(client, topic, partition, client.config.producerCompression);
    }

    ~this() {
        if (m_compression == Compression.GZIP)
            deflateEnd(m_zlibContext);
    }

    private void batcherMain() {
        for (;;) {
            MonoTime currTime = MonoTime.currTime;
            synchronized (m_queue.mutex) {
                while (!m_batchStarted || currTime < m_batchDeadline || m_bufferReserved) {
                    if (m_batchStarted && currTime < m_batchDeadline) {
                        Duration remaining = m_batchDeadline - currTime;
                        m_batchCondition.wait(remaining);
                        currTime = MonoTime.currTime;
                    } else {
                        m_batchCondition.wait();
                        currTime = MonoTime.currTime;
                    }
                }
                // perform batch
                returnCurrentBuffer();
                m_currentBuffer = null;
                m_batchStarted = false;
            }
        }
    }

    private static void setupMessageHeaders(QueueBuffer* buffer, int messageSize, int keySize, int valueSize, Compression compression) {
        import std.bitmanip, std.digest.crc;
        buffer.p[0 .. 8] = 0; // offset
        buffer.p[8 .. 12] = nativeToBigEndian(messageSize);
        buffer.p += 12; // skip header
        auto crcPtr = buffer.p;
        buffer.p[4] = 0; // MagicByte
        buffer.p[5] = cast(ubyte)compression; // Attributes
        buffer.p[6 .. 10] = nativeToBigEndian(keySize);
        buffer.p += 10 + (keySize > 0 ? keySize : 0);
        buffer.p[0 .. 4] = nativeToBigEndian(valueSize);
        buffer.p += 4 + (valueSize > 0 ? valueSize : 0);
        assert((buffer.p - crcPtr) == messageSize);
        ubyte[4] computedCrc = crc32Of(crcPtr[4 .. messageSize]); // start from 4th byte to skip the crc field
        crcPtr[0 .. 4] = nativeToBigEndian(*(cast(uint*)&computedCrc)); // set crc field
    }

    private void returnCurrentBuffer() {
        import std.algorithm, std.digest.crc, vibe.core.log;
        if (m_compression != Compression.None) {
            // Offset + MessageSize + Crc + MagicByte + Attributes + Key length + Value length
            enum int headersLen = 8 + 4 + 4 + 1 + 1 + 4 + 4;
            // maximum compressed data size, leaving room for the message headers
            int maxCompressedSize = m_client.config.producerMaxBytes - headersLen;

            void bufferCompressed(int compressedLen) {
                int messageSize = 4 + 1 + 1 + 4 + 4 + compressedLen;
                m_compressionBuffer.rewind();
                setupMessageHeaders(m_compressionBuffer, messageSize, -1, compressedLen, m_compression);
                swap(m_currentBuffer, m_compressionBuffer);
            }
            
            switch (m_compression) {
                case Compression.GZIP:
                    m_zlibContext.next_in = m_currentBuffer.buffer;
                    m_zlibContext.avail_in = cast(uint)m_currentBuffer.filled;
                    m_zlibContext.next_out = m_compressionBuffer.buffer + headersLen;
                    m_zlibContext.avail_out = maxCompressedSize;
                    int r = deflate(m_zlibContext, Z_FINISH);
                    if (r == Z_STREAM_END) {
                        bufferCompressed(cast(int)m_zlibContext.total_out);
                    } else {
                        // compressed data was bigger than the input, this may occur on uncompressible input data
                        logDebugV("Skipping GZIP compression");
                    }
                    deflateReset(m_zlibContext);
                    break;
                case Compression.Snappy:
                    size_t outLen = maxCompressedSize;
                    int r = snappy_compress(m_currentBuffer.buffer, m_currentBuffer.filled,
                        m_compressionBuffer.buffer + headersLen, &outLen);
                    if (r == SNAPPY_OK) {
                        bufferCompressed(cast(int)outLen);
                    } else {
                        // compressed data was bigger than the input, this may occur on uncompressible input data
                        logDebugV("Skipping Snappy compression");
                    }
                    break;
                default: assert(0);
            }
        }
        m_queue.returnBuffer(BufferType.Filled, m_currentBuffer);
        m_queue.notifyRequestBundler();
    }

    /// Reserves the space for the next message using specified key and value sizes. This may
    /// be used to avoid double-copy. Instead of filling the user's buffer and passing it to
    /// pushMessage(), the user may call the reserveMessage() and fill the data directly in
    /// the internal buffer using reservedKey and reservedValue slices. This function is used
    /// internally by pushMessage() function.
    /// Params:
    ///     keySize = number of bytes to reserve for the key, -1 for null key
    ///     valueSize = number of bytes to reserve for the value, -1 for the null value
    /// See_Also:
    ///     commitMessage
    void reserveMessage(int keySize, int valueSize) {
        assert(keySize >= -1);
        assert(valueSize >= -1);
        // Crc + MagicByte + Attributes + KeyLength + ValueLength
        int messageSize = 4 + 1 + 1 + 4 + 4;
        if (keySize > 0)
            messageSize += keySize;
        if (valueSize > 0)
            messageSize += valueSize;
        // Offset + MessageSize
        size_t messageSetOverhead = 8 + 4 + messageSize;
        enforce(messageSetOverhead <= m_client.config.consumerMaxBytes,
            "Message is too big to fit into message set");

        synchronized (m_queue.mutex) {
            if (!m_currentBuffer) {
                m_currentBuffer = m_queue.waitForBuffer(BufferType.Free);
                m_batchStarted = false;
                m_isFirstMessage = true;
            } else if (m_currentBuffer.remaining < messageSetOverhead) {
                returnCurrentBuffer();
                m_currentBuffer = m_queue.waitForBuffer(BufferType.Free);
                m_batchStarted = false;
                m_isFirstMessage = true;
            }
            m_bufferReserved = true;
        }

        auto p = m_currentBuffer.p + 22;
        m_reservedKey = keySize > 0 ? p[0 .. keySize] : null;
        p += m_reservedKey.length + 4;
        m_reservedValue = valueSize > 0 ? p[0 .. valueSize] : null;
        assert(p + m_reservedValue.length <= m_currentBuffer.end);

        m_messageSize = messageSize;
        m_keySize = keySize;
        m_valueSize = valueSize;
    }

    /// Commits previously reserved space and builds up the message
    /// See_Also:
    ///     reserveMessage
    void commitMessage() {
        Exception ex = m_queue.exception;
        if (ex)
            throw ex;

        setupMessageHeaders(m_currentBuffer, m_messageSize, m_keySize, m_valueSize, Compression.None);

        if (m_isFirstMessage) {
            m_isFirstMessage = false;
            synchronized (m_queue.mutex) {
                m_batchStarted = true;
                m_batchDeadline = MonoTime.currTime + m_client.config.producerBatchTimeout.msecs;
            }
        }
        m_bufferReserved = false;
        m_batchCondition.notify();
    }

    /// Pushes the message to the cluster
    /// Params:
    ///     key = the message key, may be null
    ///     value = the message value, may be null
    void pushMessage(const(ubyte)[] key, const(ubyte)[] value) {
        int keySize = key == null ? -1 : cast(int)key.length;
        int valueSize = value == null ? -1 : cast(int)value.length;
        import vibe.core.log;
        logTrace("keySize: %d, valueSize: %d", keySize, valueSize);
        reserveMessage(keySize, valueSize);
        logTrace("m_keySize: %d, m_valueSize: %d", m_reservedKey.length, m_reservedValue.length);
        if (key.length)
            m_reservedKey[] = key; // copy into buffer
        if (value.length)
            m_reservedValue[] = value; // copy into buffer
        commitMessage();
    }
}

module kafkad.protocol.serializer;

import kafkad.protocol.common;
import kafkad.config;
import kafkad.exception;
import kafkad.bundler;
import kafkad.queue;

/* serialize data up to ChunkSize, this is not zero-copy unfortunately, as vibe.d's drivers and kernel may do
 * buffering on their own, however, it should minimize the overhead of many, small write() calls to the driver */

struct Serializer {
    private {
        ubyte* chunk, p, end;
        Stream stream;
        size_t chunkSize;
    }

    this(Stream stream, size_t chunkSize) {
        chunk = cast(ubyte*)enforce(GC.malloc(chunkSize, GC.BlkAttr.NO_SCAN));
        p = chunk;
        end = chunk + chunkSize;
        this.stream = stream;
        this.chunkSize = chunkSize;
    }

    void flush() {
        if (p - chunk) {
            stream.write(chunk[0 .. p - chunk]).rethrow!StreamException("Serializer.flush() failed");
            p = chunk;
        }
    }

    void check(size_t needed) {
        pragma(inline, true);
        if (end - p < needed)
            flush();
    }

    void serialize(byte s) {
        check(1);
        *p++ = s;
    }

    void serialize(T)(T s)
        if (is(T == short) || is(T == int) || is(T == long))
    {
        check(T.sizeof);
        version (LittleEndian)
            s = swapEndian(s);
        auto pt = cast(T*)p;
        *pt++ = s;
        p = cast(ubyte*)pt;
    }

    void serializeSlice(const(ubyte)[] s) {
        if (p - chunk) {
            auto rem = end - p;
            auto toCopy = min(rem, s.length);
            core.stdc.string.memcpy(p, s.ptr, toCopy);
            p += toCopy;
            s = s[toCopy .. $];
            if (p == end)
                flush();
        }
        if (s.length) {
            if (s.length >= chunkSize) {
                stream.write(s).rethrow!StreamException("Serializer.serializeSlice() failed");
            } else {
                core.stdc.string.memcpy(chunk, s.ptr, s.length);
                p = chunk + s.length;
            }
        }
    }

    void serialize(string s) {
        assert(s.length <= short.max, "UTF8 string must not be longer than 32767 bytes");
        serialize(cast(short)s.length);
        serializeSlice(cast(ubyte[])s);
    }

    void serialize(const(ubyte)[] s) {
        assert(s.length <= int.max, "Byte array must not be larger than 4 GB"); // just in case
        serialize(cast(int)s.length);
        serializeSlice(s);
    }

    private void arrayLength(size_t length) {
        assert(length <= int.max, "Arrays must not be longer that 2^31 items"); // just in case, maybe set some configurable (and saner) limits?
        serialize(cast(int)length);
    }

    void serialize(T)(T[] s)
        if (!is(T == ubyte) && !is(T == char))
    {
        serialize!int( cast(int) s.length);
        foreach (ref a; s)
            serialize(a);
    }

    void serialize(T)(ref T s)
        if (is(T == struct))
    {
        alias Names = FieldNameTuple!T;
        foreach (N; Names)
            serialize(__traits(getMember, s, N));
    }

    private void request(size_t size, ApiKey apiKey, short apiVersion, int correlationId, string clientId) {
        size += 2 + 2 + 4 + stringSize(clientId);
        serialize(cast(int)size);
        serialize(cast(short)apiKey);
        serialize(apiVersion);
        serialize(correlationId);
        serialize(clientId);
    }

    private enum arrayOverhead = 4; // int32
    private auto stringSize(string s) { return 2 + s.length; } // int16 plus string

    // version 0
    void metadataRequest_v0(int correlationId, string clientId, string[] topics) {
        auto size = arrayOverhead;
        foreach (t; topics)
            size += stringSize(t);
        request(size, ApiKey.MetadataRequest, 0, correlationId, clientId);
        arrayLength(topics.length);
        foreach (t; topics)
            serialize(t);
    }

    // version 0
    void fetchRequest_v0(int correlationId, string clientId, in Configuration config, RequestBundler requestBundler) {
        size_t topics = 0;
        auto size = 4 + 4 + 4 + arrayOverhead;
        Topic* t = requestBundler.requestTopicsFront;
        while (t) {
            size += stringSize(t.topic) + arrayOverhead + t.partitionsInRequest * (4 + 8 + 4);
            ++topics;
            t = t.next;
        }
        request(size, ApiKey.FetchRequest, 0, correlationId, clientId);
        serialize!int(-1); // ReplicaId
        serialize!int(config.consumerMaxWaitTime); // MaxWaitTime
        serialize!int(config.consumerMinBytes); // MinBytes
        arrayLength(topics);
        t = requestBundler.requestTopicsFront;
        while (t) {
            serialize(t.topic);
            arrayLength(t.partitionsInRequest);
            Partition* p = t.requestPartitionsFront;
            while (p) {
                serialize(p.partition);
                serialize(p.queue.offset);
                serialize!int(config.consumerMaxBytes); // MaxBytes
                p = p.next;
            }
            t = t.next;
        }
    }

    // version 0
    void produceRequest_v0(int correlationId, string clientId, in Configuration config, RequestBundler requestBundler) {
        size_t topics = 0;
        auto size = 2 + 4 + arrayOverhead;
        Topic* t = requestBundler.requestTopicsFront;
        while (t) {
            size += stringSize(t.topic) + arrayOverhead + t.partitionsInRequest * (4 + 4);
            Partition* p = t.requestPartitionsFront;
            while (p) {
                synchronized (p.queue.mutex) {
                    p.buffer = p.queue.getBuffer(BufferType.Filled);
                }
                size += p.buffer.filled;
                p = p.next;
            }
            ++topics;
            t = t.next;
        }
        import vibe.core.log; logDebug("produce request size: %d", size);
        request(size, ApiKey.ProduceRequest, 0, correlationId, clientId);
        serialize!short(config.producerRequiredAcks); // RequiredAcks
        serialize!int(config.produceRequestTimeout); // Timeout
        arrayLength(topics);
        t = requestBundler.requestTopicsFront;
        while (t) {
            serialize(t.topic);
            arrayLength(t.partitionsInRequest);
            Partition* p = t.requestPartitionsFront;
            while (p) {
                serialize!int(p.partition);
                serialize!int(cast(int)p.buffer.filled);
                serializeSlice(p.buffer.filledSlice);
                synchronized (p.queue.mutex) {
                    p.buffer.rewind();
                    p.queue.returnBuffer(BufferType.Free, p.buffer);
                    p.queue.condition.notify();
                }
                p = p.next;
            }
            t = t.next;
        }
    }

    // version 0
    void offsetRequest_v0(int correlationId, string clientId, OffsetRequestParams_v0 params) {
        auto size = 4 + arrayOverhead;
        foreach (ref t; params.topics) {
            size += stringSize(t.topic) + arrayOverhead + t.partitions.length * (4 + 8 + 4);
        }
        request(size, ApiKey.OffsetRequest, 0, correlationId, clientId);
        serialize(params);
    }
}

struct OffsetRequestParams_v0 {
    static struct PartTimeMax {
        int partition;
        long time;
        int maxOffsets;
    }
    static struct Topic {
        string topic;
        PartTimeMax[] partitions;
    }
    int replicaId;
    Topic[] topics;
}

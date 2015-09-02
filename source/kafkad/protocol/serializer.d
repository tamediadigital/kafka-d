module kafkad.protocol.serializer;

import kafkad.protocol.common;
import kafkad.config;
import kafkad.exception;
import kafkad.consumer.group;

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
        assert(p - chunk);
        stream.write(chunk[0 .. p - chunk]).rethrow!StreamException("Serializer.flush() failed");
        p = chunk;
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

    private void serializeSlice(const(ubyte)[] s) {
        auto slice = s;
        if (slice.length > chunkSize) {
            if (p - chunk)
                flush();
            while (slice.length > chunkSize) {
                stream.write(slice[0 .. chunkSize]).rethrow!StreamException("Serializer.serializeSlice() failed");
                slice = slice[chunkSize .. $];
            }
        }
        check(slice.length);
        core.stdc.string.memcpy(p, slice.ptr, slice.length);
        p += slice.length;
    }

    void serialize(string s) {
        enforce(s.length <= short.max, "UTF8 string must not be longer than 32767 bytes");
        serialize(cast(short)s.length);
        serializeSlice(cast(ubyte[])s);
    }

    void serialize(const(ubyte)[] s) {
        enforce(s.length <= int.max, "Byte array must not be larger than 4 GB"); // just in case
        serialize(cast(int)s.length);
        serializeSlice(s);
    }

    private void arrayLength(size_t length) {
        enforce(length <= int.max, "Arrays must not be longer that 2^31 items"); // just in case, maybe set some configurable (and saner) limits?
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
    void fetchRequest_v0(int correlationId, string clientId, in Configuration config, QueueGroup queueGroup) {
        size_t topics = 0;
        auto size = 4 + 4 + 4 + arrayOverhead;
        GroupTopic* t = queueGroup.fetchRequestTopicsFront;
        while (t) {
            size += stringSize(t.topic) + arrayOverhead + t.partitionsInFetchRequest * (4 + 8 + 4);
            ++topics;
            t = t.next;
        }
        request(size, ApiKey.FetchRequest, 0, correlationId, clientId);
        serialize!int(-1); // ReplicaId
        serialize!int(config.consumerMaxWaitTime); // MaxWaitTime
        serialize!int(config.consumerMinBytes); // MinBytes
        arrayLength(topics);
        t = queueGroup.fetchRequestTopicsFront;
        while (t) {
            serialize(t.topic);
            arrayLength(t.partitionsInFetchRequest);
            GroupPartition* p = t.fetchRequestPartitionsFront;
            while (p) {
                serialize(p.queue.consumer.partition);
                serialize(p.queue.offset);
                serialize!int(config.consumerMaxBytes); // MaxBytes
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

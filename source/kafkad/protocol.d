module kafkad.protocol;

/*
 * Kafka requests are always initiated by clients so we only need serializers for requests and deserializers for responses
 * 
 * Kafka 0.8.x network protocol is described here: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
 */

import core.memory;
import std.algorithm : min;
import std.bitmanip;
import std.exception;
import std.traits;
import vibe.core.stream;

package:

immutable size_t ChunkSize = 4096; // TODO: configurability

/* serialize data up to ChunkSize, this is not zero-copy unfortunately, as vibe.d's drivers and kernel may do
 * buffering on their own, however, it should minimize the overhead of many, small write() calls to the driver */

enum ApiKey : short
{
    ProduceRequest = 0,
    FetchRequest = 1,
    OffsetRequest = 2,
    MetadataRequest = 3,
    //Non-user facing control APIs = 4-7
    OffsetCommitRequest = 8,
    OffsetFetchRequest = 9,
    ConsumerMetadataRequest = 10
}

struct Serializer
{
    private
    {
        ubyte* chunk, p, end;
        Stream stream;
    }

    this(Stream stream)
    {
        chunk = cast(ubyte*) enforce(GC.malloc(ChunkSize, GC.BlkAttr.NO_SCAN));
        p = chunk;
        end = chunk + ChunkSize;
        this.stream = stream;
    }

    void flush()
    {
        assert(p - chunk);
        stream.write(chunk[0 .. p - chunk]);
        p = chunk;
    }

    void check(size_t needed)
    {
        pragma(inline, true);
        if (end - p < needed)
            flush();
    }

    void serialize(byte s)
    {
        check(1);
        *p++ = s;
    }

    void serialize(T)(T s) if (is(T == short) ||  is(T == int) ||  is(T == long))
    {
        check(T.sizeof);
        version (LittleEndian)
            s = swapEndian(s);
        auto pt = cast(T*) p;
        *pt++ = s;
        p = cast(ubyte*) pt;
    }

    private void serializeSlice(ubyte[] s)
    {
        auto slice = s;
        while (slice.length > ChunkSize)
        {
            flush();
            stream.write(slice[0 .. ChunkSize]);
            slice = slice[ChunkSize .. $];
        }
        check(slice.length);
        core.stdc.string.memcpy(p, s.ptr, s.length);
        p += s.length;
    }

    void serialize(string s)
    {
        enforce(s.length <= short.max, "UTF8 string must not be longer than 32767 bytes");
        serialize(cast(short) s.length);
        serializeSlice(cast(ubyte[]) s);
    }

    void serialize(ubyte[] s)
    {
        enforce(s.length <= int.max, "Byte array must not be larger than 4 GB"); // just in case
        serialize(cast(int) s.length);
        serializeSlice(s);
    }

    private void arrayLength(size_t length)
    {
        enforce(length <= int.max, "Arrays must not be longer that 2^31 items"); // just in case, maybe set some configurable (and saner) limits?
        serialize(cast(int) length);
    }

    private void request(size_t size, ApiKey apiKey, short apiVersion, int correlationId, string clientId)
    {
        size += 2 + 2 + 4 + stringSize(clientId);
        serialize(cast(int) size);
        serialize(cast(short) apiKey);
        serialize(apiVersion);
        serialize(correlationId);
        serialize(clientId);
    }

    private enum arrayOverhead = 4; // int32
    private auto stringSize(string s)
    {
        return 2 + s.length;
    }
 // int16 plus string
    // version 0
    void metadataRequest_v0(int correlationId, string clientId, string[] topics)
    {
        auto size = arrayOverhead;
        foreach (t; topics)
            size += stringSize(t);
        request(size, ApiKey.MetadataRequest, 0, correlationId, clientId);
        arrayLength(topics.length);
        foreach (t; topics)
            serialize(t);
        flush();
    }
}

// read data up to ChunkSize and then deserialize

struct Deserializer
{
    private
    {
        ubyte* chunk, p, end;
        Stream stream;
        size_t remaining; // bytes remaining in current message
    }

    this(Stream stream)
    {
        chunk = cast(ubyte*) enforce(GC.malloc(ChunkSize, GC.BlkAttr.NO_SCAN));
        p = chunk;
        end = chunk;
        this.stream = stream;
    }

    void beginMessage(size_t size)
    {
        remaining = size;
    }

    // todo: test
    void skipMessage(size_t size)
    {
        auto tail = end - p;
        if (size <= tail)
        {
            p += size;
        }
        else
        {
            size -= tail;
            end = chunk + size % ChunkSize;
            p = end;
            while (size)
            {
                auto toRead = min(size, ChunkSize);
                stream.read(chunk[0 .. toRead]);
                size -= toRead;
            }

        }
    }

    void read()
    {
        assert(remaining);
        auto tail = end - p;
        if (tail && tail < ChunkSize)
            core.stdc.string.memmove(chunk, p, tail);
        // read up to remaining if it's smaller than chunk size, it will prevent blocking if read buffer is empty
        // consider: reading up to vibe's leastSize();
        auto toRead = min(ChunkSize, tail + remaining);
        stream.read(chunk[tail .. toRead]);
        p = chunk;
        end = chunk + toRead;
        remaining -= toRead - tail;
    }

    void check(size_t needed)
    {
        pragma(inline, true);
        if (end - p < needed)
            read();
    }

    void getMessage(out int size, out int correlationId)
    {
        remaining = 8;
        check(8);
        deserialize(size);
        deserialize(correlationId); // todo: skip unknown correlationID messages
        size -= 4; // subtract correlationId overhead
    }

    void deserialize(out byte s)
    {
        check(1);
        s = *p++;
    }

    void deserialize(T)(out T s) if (is(T == short) ||  is(T == int) ||  is(T == long))
    {
        check(T.sizeof);
        auto pt = cast(T*) p;
        s = *pt++;
        p = cast(ubyte*) pt;
        version (LittleEndian)
            s = swapEndian(s);
    }

    private void deserializeSlice(ubyte[] s)
    {
        auto slice = s;
        auto tail = min(slice.length, end - p);
        core.stdc.string.memcpy(slice.ptr, p, tail);
        p += tail;
        slice = slice[tail .. $];
        while (slice.length)
        {
            read();
            auto cnt = end - p;
            core.stdc.string.memcpy(slice.ptr, p, cnt);
            slice = slice[cnt .. $];
        }
    }

    void deserializeBytes(T)(out ubyte[] s)
    {
        T len;
        deserialize(len);
        assert(len >= 0);
        s = new ubyte[len];
        deserializeSlice(s);
    }

    void deserialize(out string s)
    {
        ubyte[] b;
        deserializeBytes!short(b);
        s = cast(string) b;
    }

    void deserialize(out ubyte[] s)
    {
        deserializeBytes!int(s);
    }

    void deserialize(T)(out T[] s) if (!is(T == ubyte) &&  !is(T == char))
    {
        check(4);
        int len;
        deserialize(len);
        assert(len >= 0);
        s = new T[len];
        foreach (ref a; s)
            deserialize(a);
    }

    void deserialize(T)(out T s) if (is(T == struct))
    {
        alias Names = FieldNameTuple!T;
        foreach (N; Names)
            deserialize(__traits(getMember, s, N));
    }

    auto metadataResponse_v0()
    {
        MetadataResponse r;
        deserialize(r);
        return r;
    }
}

struct Broker
{
    int id;
    string host;
    int port;
}

struct PartitionMetadata
{
    short errorCode;
    int id;
    int leader;
    int[] replicas;
    int[] isr;
}

struct TopicMetadata
{
    short errorCode;
    string name;
    PartitionMetadata[] partitions;
}

struct MetadataResponse
{
    Broker[] brokers;
    TopicMetadata[] topics;
}

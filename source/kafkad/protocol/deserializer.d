module kafkad.protocol.deserializer;

import kafkad.protocol.common;
import kafkad.protocol.metadata;
import kafkad.exception;

// read data up to ChunkSize and then deserialize

struct Deserializer {
    private {
        ubyte* chunk, p, end;
        Stream stream;
        size_t remaining; // bytes remaining in current message
        size_t chunkSize;
    }
    
    this(Stream stream, size_t chunkSize) {
        chunk = cast(ubyte*)enforce(GC.malloc(chunkSize, GC.BlkAttr.NO_SCAN));
        p = chunk;
        end = chunk;
        this.stream = stream;
        this.chunkSize = chunkSize;
    }

    // must be called before each message (needed for correct chunk processing)
    void beginMessage(size_t size) {
        remaining = size;
    }

    void endMessage() {
        if (remaining)
            skipBytes(remaining);
    }

    // todo: test
    void skipBytes(size_t size) {
        auto tail = end - p;
        if (size <= tail) {
            p += size;
        } else {
            size -= tail;
            end = chunk + size % chunkSize;
            p = end;
            while (size) {
                auto toRead = min(size, chunkSize);
                stream.read(chunk[0 .. toRead]).rethrow!StreamException("Deserializer.skipBytes() failed");
                size -= toRead;
            }
        }
    }

    void read() {
        assert(remaining);
        auto tail = end - p;
        if (tail && tail < chunkSize)
            core.stdc.string.memmove(chunk, p, tail);
        // read up to remaining if it's smaller than chunk size, it will prevent blocking if read buffer is empty
        // consider: reading up to vibe's leastSize();
        auto toRead = min(chunkSize, tail + remaining);
        stream.read(chunk[tail .. toRead]).rethrow!StreamException("Deserializer.read() failed");
        p = chunk;
        end = chunk + toRead;
        remaining -= toRead - tail;
    }

    void check(size_t needed) {
        pragma(inline, true);
        if (end - p < needed)
            read();
    }

    void getMessage(out int size, out int correlationId) {
        remaining = 8;
        check(8);
        deserialize(size);
        deserialize(correlationId);
        size -= 4; // subtract correlationId overhead
    }

    // returns chunk slice up to remaining bytes
    ubyte[] getChunk(size_t needed) {
        needed = min(needed, chunkSize);
        check(needed);
        auto slice = p[0 .. needed];
        p += needed;
        return slice;
    }

    void deserialize(out byte s) {
        check(1);
        s = *p++;
    }
    
    void deserialize(T)(out T s)
        if (is(T == short) || is(T == int) || is(T == long))
    {
        check(T.sizeof);
        auto pt = cast(T*)p;
        s = *pt++;
        p = cast(ubyte*)pt;
        version (LittleEndian)
            s = swapEndian(s);
    }

    void deserialize(T)(out T s)
        if (is(T == enum))
    {
        OriginalType!T v;
        deserialize(v);
        s = cast(T)v;
    }

    void deserializeSlice(ubyte[] s) {
        auto tail = end - p; // amount available in the chunk
        if (s.length <= tail) {
            core.stdc.string.memcpy(s.ptr, p, s.length);
            p += s.length;
        } else {
            core.stdc.string.memcpy(s.ptr, p, tail);
            p += tail;
            s = s[tail .. $];
            if (s.length >= chunkSize) {
                stream.read(s).rethrow!StreamException("Deserializer.deserializeSlice() failed");
                remaining -= s.length;
            } else {
                read();
                assert(end - p >= s.length);
                core.stdc.string.memcpy(s.ptr, p, s.length);
				p += s.length;
            }
        }
    }

    void deserializeBytes(T)(out ubyte[] s) {
        T len;
        deserialize(len);
        assert(len >= 0);
        s = new ubyte[len];
        deserializeSlice(s);
    }

    void deserialize(out string s) {
        ubyte[] b;
        deserializeBytes!short(b);
        s = cast(string)b;
    }

    void deserialize(out ubyte[] s) {
        deserializeBytes!int(s);
    }

    void deserialize(T)(out T[] s)
        if (!is(T == ubyte) && !is(T == char))
    {
        check(4);
        int len;
        deserialize(len);
        assert(len >= 0);
        s = new T[len];
        foreach (ref a; s)
            deserialize(a);
    }

    void deserialize(T)(out T s)
        if (is(T == struct))
    {
        alias Names = FieldNameTuple!T;
        foreach (N; Names)
            deserialize(__traits(getMember, s, N));
    }

    auto metadataResponse_v0() {
        Metadata r;
        deserialize(r);
        return r;
    }

    auto offsetResponse_v0() {
        OffsetResponse_v0 r;
        deserialize(r);
        return r;
    }
}

struct OffsetResponse_v0 {
    static struct PartitionOffsets {
        int partition;
        short errorCode;
        long[] offsets;
    }
    static struct Topic {
        string topic;
        PartitionOffsets[] partitions;
    }
    Topic[] topics;
}

module kafkad.protocol.deserializer;

import kafkad.protocol.common;
import kafkad.protocol.fetch;
import kafkad.exception;

// read data up to ChunkSize and then deserialize

struct Deserializer {
    private {
        ubyte* chunk, p, end;
        Stream stream;
        size_t remaining; // bytes remaining in current message
    }
    
    this(Stream stream) {
        chunk = cast(ubyte*)enforce(GC.malloc(ChunkSize, GC.BlkAttr.NO_SCAN));
        p = chunk;
        end = chunk;
        this.stream = stream;
    }

    // must be called before each message (needed for correct chunk processing)
    void beginMessage(size_t size) {
        remaining = size;
    }

    // todo: test
    void skipBytes(size_t size) {
        auto tail = end - p;
        if (size <= tail) {
            p += size;
        } else {
            size -= tail;
            end = chunk + size % ChunkSize;
            p = end;
            while (size) {
                auto toRead = min(size, ChunkSize);
                stream.read(chunk[0 .. toRead]).rethrow!StreamException("Deserializer.skipBytes() failed");
                size -= toRead;
            }
        }
    }

    void read() {
        assert(remaining);
        auto tail = end - p;
        if (tail && tail < ChunkSize)
            core.stdc.string.memmove(chunk, p, tail);
        // read up to remaining if it's smaller than chunk size, it will prevent blocking if read buffer is empty
        // consider: reading up to vibe's leastSize();
        auto toRead = min(ChunkSize, tail + remaining);
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
        deserialize(correlationId); // todo: skip unknown correlationID messages
        size -= 4; // subtract correlationId overhead
    }

    // returns chunk slice up to remaining bytes
    ubyte[] getChunk(size_t needed) {
        needed = min(needed, ChunkSize);
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

    private void deserializeSlice(ubyte[] s) {
        auto slice = s;
        auto tail = min(slice.length, end - p);
        core.stdc.string.memcpy(slice.ptr, p, tail);
        p += tail;
        slice = slice[tail .. $];
        while (slice.length) {
            read();
            auto cnt = end - p;
            core.stdc.string.memcpy(slice.ptr, p, cnt);
            slice = slice[cnt .. $];
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

    auto fetchResponse_v0() {
        int numtopics;
        deserialize(numtopics);
        return TopicRange(&this, numtopics);
    }
}

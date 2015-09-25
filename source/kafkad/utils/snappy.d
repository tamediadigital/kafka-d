module kafkad.utils.snappy;

import std.bitmanip;

nothrow extern(C):

enum {
    SNAPPY_OK = 0,
    SNAPPY_INVALID_INPUT = 1,
    SNAPPY_BUFFER_TOO_SMALL = 2
}

int snappy_compress(const ubyte* input,
    size_t input_length,
    ubyte* compressed,
    size_t* compressed_length);

int snappy_uncompress(const ubyte* compressed,
    size_t compressed_length,
    ubyte* uncompressed,
    size_t* uncompressed_length);

int snappy_uncompressed_length(const ubyte* compressed,
    size_t compressed_length,
    size_t* result);

int snappy_java_uncompress(const ubyte* compressed,
    size_t compressed_length,
    ubyte* uncompressed,
    size_t* uncompressed_length)
{
    // Snappy-Java adds its own framing: Header [ChunkLen Chunk]
    // Header = Magic (8 bytes) Version (4 bytes) Compatible (4 bytes)
    enum headerLen = 8 + 4 + 4;
    if (compressed_length < headerLen + 4) // header + chunk length
        return SNAPPY_INVALID_INPUT;
    // check magic value
    enum ubyte[8] magic = [0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0];
    ubyte* p = cast(ubyte*)compressed;
    auto cmagic = cast(ubyte[8]*)p;
    if (*cmagic != magic)
        return SNAPPY_INVALID_INPUT;
    p += headerLen;
    compressed_length -= headerLen;
    if (!compressed_length)
        return SNAPPY_INVALID_INPUT;
    size_t wholeUncompressedLen = 0, chunkCount = 0;
    while (compressed_length) {
        if (compressed_length < 4)
            return SNAPPY_INVALID_INPUT;
        auto chunkLen = bigEndianToNative!uint(p[0 .. 4]);
        if (!chunkLen)
            return SNAPPY_INVALID_INPUT;
        p += 4;
        compressed_length -= 4;
        if (chunkLen > compressed_length)
            return SNAPPY_INVALID_INPUT;
        size_t chunkUncompressedLen;
        if (snappy_uncompressed_length(p, chunkLen, &chunkUncompressedLen) != SNAPPY_OK || !chunkUncompressedLen)
            return SNAPPY_INVALID_INPUT;
        wholeUncompressedLen += chunkUncompressedLen;
        p += chunkLen;
        compressed_length -= chunkLen;
        ++chunkCount;
    }
    if (wholeUncompressedLen > *uncompressed_length)
        return SNAPPY_BUFFER_TOO_SMALL;
    p = cast(ubyte*)compressed + headerLen;
    while (chunkCount--) {
        auto chunkLen = bigEndianToNative!uint(p[0 .. 4]);
        p += 4;
        size_t rem = *uncompressed_length;
        if (snappy_uncompress(p, chunkLen, uncompressed, &rem) != SNAPPY_OK)
            return SNAPPY_INVALID_INPUT;
        p += chunkLen;
        uncompressed += rem;
        *uncompressed_length -= rem;
    }
    *uncompressed_length = wholeUncompressedLen;
    return SNAPPY_OK;
}
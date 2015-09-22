module kafkad.utils.snappy;

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
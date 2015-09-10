module kafkad.exception;

import std.exception;

mixin template ExceptionCtorMixin() {
    @nogc @safe pure nothrow this(string msg, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
        super(msg, file, line, next);
    }
    @nogc @safe pure nothrow this(string msg, Throwable next, string file = __FILE__, size_t line = __LINE__) {
        super(msg, file, line, next);
    }
}

/// used to mask vibe.d's Exceptions related to connection establishment
class ConnectionException : Exception {
    mixin ExceptionCtorMixin;
}

/// used to mask vibe.d's Exceptions related to stream I/O
class StreamException : Exception {
    mixin ExceptionCtorMixin;
}

class MetadataException : Exception {
    mixin ExceptionCtorMixin;
}

class ProtocolException : Exception {
    mixin ExceptionCtorMixin;
}

class CrcException : Exception {
    mixin ExceptionCtorMixin;
}

/// Catches any exception in the expression and throws a new one specified by E and args
/// Examples:
/// -----
/// import std.conv;
/// class MyException : Exception { ... }
/// "x".to!int().rethrow!MyException("My message");
/// -----
T rethrow(E : Exception, T, Args)(lazy scope T expression, Args args) {
    try {
        return expression();
    } catch (Exception) {
        throw new E(args);
    }
}

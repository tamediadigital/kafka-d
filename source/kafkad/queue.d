module kafkad.queue;

import kafkad.worker;
import kafkad.bundler;
import kafkad.utils.lists;
import vibe.core.sync;
import std.bitmanip;
import std.exception;
import core.memory;

// Represents a single buffer of the worker queue
struct QueueBuffer {
    ubyte* buffer, p, end;
    size_t messageSetSize;
    Exception exception;
    long requestedOffset;

    QueueBuffer* next;

    this(size_t size) {
        buffer = cast(ubyte*)enforce(GC.malloc(size, GC.BlkAttr.NO_SCAN));
        p = buffer;
        end = buffer + size;
        exception = null;
    }

    this(Exception ex) {
        exception = ex;
    }

    @property size_t filled() {
        return p - buffer;
    }

    @property size_t remaining() {
        return end - p;
    }

    @property ubyte[] filledSlice() {
        return buffer[0 .. filled];
    }

    void rewind() {
        p = buffer;
    }

    long findNextOffset() {
        ubyte* p = this.p;
        ubyte* end = this.p + messageSetSize;
        long offset = -2;
        while (end - p > 12) {
            offset = bigEndianToNative!long(p[0 .. 8]);
            int messageSize = bigEndianToNative!int(p[8 .. 12]);
            p += 12 + messageSize;
            if (p > end) {
                // this is last, partial message
                return offset;
            }
        }
        return offset + 1;
    }
}

enum BufferType { Free, Filled }

// Holds the buffers for the workers
final class Queue {
    private {
        IWorker m_worker;
        List!QueueBuffer[2] m_buffers;
        TaskMutex m_mutex;
        TaskCondition m_condition;
        bool m_requestPending;
        RequestBundler m_bundler;
        long m_offset;
    }

    // this is also updated in the fetch/push task
    bool requestPending() { return m_requestPending; }
    bool requestPending(bool v) { return m_requestPending = v; }

    @property auto worker() { return m_worker; }

    @property auto mutex() { return m_mutex; }
    @property auto condition() { return m_condition; }

    @property auto requestBundler() { return m_bundler; }
    @property auto requestBundler(RequestBundler v) { return m_bundler = v; }

    @property auto offset() { return m_offset; }
    @property auto offset(long v) { return m_offset = v; }

    this(IWorker worker, size_t bufferCount, size_t bufferSize) {
        import std.algorithm : max;
        m_worker = worker;
        auto m_freeBufferCount = max(2, bufferCount); // at least 2
        foreach (n; 0 .. m_freeBufferCount) {
            auto qbuf = new QueueBuffer(bufferSize);
            m_buffers[BufferType.Free].pushBack(qbuf);
        }
        m_mutex = new TaskMutex();
        m_condition = new TaskCondition(m_mutex);
        m_requestPending = false;
        m_bundler = null;
    }

    bool hasBuffer(BufferType bufferType) {
        return !m_buffers[bufferType].empty;
    }

    QueueBuffer* getBuffer(BufferType bufferType) {
        return m_buffers[bufferType].popFront();
    }

    QueueBuffer* waitForBuffer(BufferType bufferType) {
        List!QueueBuffer* buffers = &m_buffers[bufferType];
        while (buffers.empty)
            m_condition.wait();
        QueueBuffer* buffer = buffers.popFront();
        if (buffer.exception)
            throw buffer.exception;
        return buffer;
    }

    void returnBuffer(BufferType bufferType, QueueBuffer* buffer) {
        assert(buffer);
        m_buffers[bufferType].pushBack(buffer);
    }

    void notifyRequestBundler() {
        // notify the fetch/push task that there are ready buffers
        // the fetch/push task will then make a batch request for all queues with ready buffers
        // do not notify the task if there is a pending request for this queue
        if (m_bundler && !m_requestPending) {
            synchronized (m_bundler.mutex) {
                m_bundler.queueHasReadyBuffers(this);
            }
            m_requestPending = true;
        }
    }
}

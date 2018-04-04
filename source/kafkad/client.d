module kafkad.client;

import kafkad.connection;
import kafkad.protocol;
import kafkad.worker;
import kafkad.queue;
import core.time;
import std.container.dlist;
import std.exception;
import std.conv;
public import kafkad.config;
public import kafkad.consumer;
public import kafkad.producer;
public import kafkad.exception;

struct BrokerAddress {
    string host;
    ushort port;

    this(string host, ushort port)
    {
        this.host = host;
        this.port = port;
    }

    this(string address)
    {
        import std.algorithm : splitter;
        import std.array : array;
        auto splitted = address.splitter(":").array;
        enforce(splitted.length == 2, "BrokerAddress supplied is incomplete");
        this.host = splitted[0];
        this.port = splitted[1].to!ushort;
    }
}

unittest
{
    string hostname = "127.0.0.1";
    ushort port = 9292;
    auto address = hostname ~ ":" ~ port.to!string;
    auto b = BrokerAddress(address);
    assert(b.host == hostname, "hostname in BrokerAddress constructor not working");
    assert(b.port == port, "port in BrokerAddress constructor not working");
}

/// The client acts as a router between brokers, consumers and producers. Consumers and producers
/// connect to the client and it handles connections to the brokers for them. It transparently handles
/// connection failures, leader switches and translates topic/partitions to respective broker connections.
class Client {
    enum __isWeakIsolatedType = true; // needed to pass this type between vibe.d's tasks
    private {
        import vibe.core.net : NetworkAddress;
        import vibe.core.sync : TaskCondition, TaskMutex;
        import vibe.core.task : Task;

        Configuration m_config;
        BrokerAddress[] m_bootstrapBrokers;
        string m_clientId;
        BrokerConnection[NetworkAddress] m_conns;
        NetworkAddress[int] m_hostCache; // broker id to NetworkAddress cache
        Metadata m_metadata;
        bool m_gotMetadata;

        DList!IWorker m_workers, m_brokerlessWorkers;
        TaskMutex m_mutex;
        TaskCondition m_brokerlessWorkersEmpty;
        Task m_connectionManager;
    }

    @property auto clientId() { return m_clientId; }
    @property auto clientId(string v) { return m_clientId = v; }

    @property ref const(Configuration) config() { return m_config; }

    import std.string, std.process;
    this(BrokerAddress[] bootstrapBrokers, string clientId = format("kafka-d-%d",thisProcessID),
        Configuration config = Configuration())
    {
        import vibe.core.core : runTask;

        m_config = config;
        enforce(bootstrapBrokers.length);
        m_bootstrapBrokers = bootstrapBrokers;
        m_clientId = clientId;
        m_mutex = new TaskMutex();
        m_brokerlessWorkersEmpty = new TaskCondition(m_mutex);
        m_connectionManager = runTask(&connectionManagerMain);
        m_gotMetadata = false;
    }

    /// Refreshes the metadata and stores it in the cache. Call it before using the getTopics/getPartitions to get the most recent metadata.
    /// Metadata is also refreshed internally on the first use and on each consumer/producer failure.
    void refreshMetadata() {
        import vibe.core.core : sleep;
        import vibe.core.log : logDebug, logWarn;

        synchronized (m_mutex) {
            Exception lastException = null;
            auto remainingRetries = m_config.metadataRefreshRetryCount;
            while (!m_config.metadataRefreshRetryCount || remainingRetries--) {
                foreach (brokerAddr; m_bootstrapBrokers) {
                    try {
                        auto conn = getConn(brokerAddr);
                        auto host = conn.addr;
                        m_metadata = conn.getMetadata([]);
                        enforce(m_metadata.brokers.length, new ConnectionException("Empty metadata, this may indicate there are no defined topics in the cluster"));
                        m_hostCache = null; // clear the cache

                        int bootstrapBrokerId = -1;
                        // look up this host in the metadata to obtain its node id
                        // also, fill the nodeid cache
                        foreach (ref b; m_metadata.brokers) {
                            enforce(b.port >= 0 && b.port <= ushort.max);
                            auto bhost = resolveBrokerAddr(BrokerAddress(b.host, cast(ushort)b.port));
                            if (bhost == host)
                                bootstrapBrokerId = b.id;
                            m_hostCache[b.id] = bhost;
                        }

                        if(bootstrapBrokerId < 0)
                        {
                            import std.range : takeOne, front;
                            bootstrapBrokerId = m_metadata.brokers.takeOne.front.id;
                            logWarn("Your Bootstrap Broker is not in the advertised brokers! using broker %s from available Brokers: %s", bootstrapBrokerId, m_metadata.brokers);
                        }

                        conn.id = bootstrapBrokerId;

                        debug {
                            logDebug("Broker list:");
                            foreach (ref b; m_metadata.brokers) {
                                logDebug("\tBroker ID: %d, host: %s, port: %d", b.id, b.host, b.port);
                            }
                            logDebug("Topic list:");
                            foreach (ref t; m_metadata.topics) {
                                logDebug("\tTopic: %s, partitions:", t.topic);
                                foreach (ref p; t.partitions) {
                                    logDebug("\t\tPartition: %d, Leader ID: %d, Replicas: %s, In sync replicas: %s",
                                        p.id, p.leader, p.replicas, p.isr);
                                }
                            }
                        }

                        m_gotMetadata = true;
                        return;
                    } catch (ConnectionException ex) {
                        lastException = ex;
                        continue;
                    }
                }
                sleep(m_config.metadataRefreshRetryTimeout.msecs);
            }
            // fatal error, we couldn't get the new metadata from the bootstrap brokers
            assert(lastException);
            throw lastException;
        }
    }

    private NetworkAddress resolveBrokerAddr(BrokerAddress brokerAddr) {
        import vibe.core.net : resolveHost;

        auto netAddr = resolveHost(brokerAddr.host).rethrow!ConnectionException("Could not resolve host " ~ brokerAddr.host);
        netAddr.port = brokerAddr.port;
        return netAddr;
    }

    private BrokerConnection getConn(BrokerAddress brokerAddr) {
        auto netAddr = resolveBrokerAddr(brokerAddr);
        return getConn(netAddr);
    }

    private BrokerConnection getConn(NetworkAddress netAddr) {
        import vibe.core.net : connectTCP;

        auto pconn = netAddr in m_conns;
        if (!pconn) {
            auto tcpConn = connectTCP(netAddr).rethrow!ConnectionException("TCP connect to address " ~ netAddr.toString() ~ " failed");
            auto conn = new BrokerConnection(this, tcpConn);
            m_conns[netAddr] = conn;
            pconn = &conn;
        }
        return *pconn;
    }

    private auto getConn(int id) {
        assert(id in m_hostCache);
        auto netAddr = m_hostCache[id];
        auto conn = getConn(netAddr);
        conn.id = id;
        return conn;
    }

    string[] getTopics() {
        if (!m_gotMetadata)
            refreshMetadata();
        string[] topics;
        foreach (ref t; m_metadata.topics) {
            topics ~= t.topic;
        }
        return topics;
    }

    int[] getPartitions(string topic) {
        if (!m_gotMetadata)
            refreshMetadata();
        int[] partitions;
        auto tm = m_metadata.findTopicMetadata(topic);
        foreach (ref p; tm.partitions) {
            partitions ~= p.id;
        }
        return partitions;
    }

    // This task tries to reconnect consumers and producers (workers) to the brokers in the background.
    // When the connection fails or the leader is changed for a partition, the worker needs to switch
    // the connection to the other broker. Worker is added to the brokerlessWorkers list each time
    // the connection becomes invalid (it's also added upon the worker class instantiation).
    // In such situation, consumer queue is still valid and may be processed by the user's task. It may happen
    // that the connection is switched before the queue is exhausted, and the new connection fills the queue up
    // again in a short time, so that the consumer doesn't need to wait for the messages at all. For the consumer,
    // it would be completely transparent.
    private void connectionManagerMain() {
        import vibe.core.core : sleep;

    mainLoop:
        for (;;) {
            IWorker worker;
            synchronized (m_mutex) {
                while (m_brokerlessWorkers.empty)
                    m_brokerlessWorkersEmpty.wait();
                worker = m_brokerlessWorkers.front;
                m_brokerlessWorkers.removeFront();
            }

            PartitionMetadata pm;

            // get the new partition metadata and wait for leader election if needed
            auto remainingRetries = m_config.leaderElectionRetryCount;
            while (!m_config.leaderElectionRetryCount || remainingRetries--) {
                try {
                    refreshMetadata();
                    pm = m_metadata.findTopicMetadata(worker.topic).
                                    findPartitionMetadata(worker.partition);
                } catch (MetadataException ex) {
                    // no topic and/or partition on this broker
                    worker.throwException(ex);
                    continue mainLoop;
                } catch (ConnectionException ex) {
                    // couldn't connect to the broker
                    worker.throwException(ex);
                    continue mainLoop;
                }
                if (pm.leader >= 0)
                    break;
                sleep(m_config.leaderElectionRetryTimeout.msecs);
            }

            if (pm.leader < 0) {
                // all retries failed, we still dont have a leader for the consumer's topic/partition
                worker.throwException(new Exception("Leader election timed out"));
                continue;
            }

            try {
                BrokerConnection conn = getConn(pm.leader);
                auto consumer = cast(Consumer)worker;
                if (consumer) {
                    if (consumer.queue.offset < 0) {
                        // get earliest or latest offset
                        auto offset = conn.getStartingOffset(consumer.topic, consumer.partition, consumer.queue.offset);
                        consumer.queue.offset = offset;
                    }
                    conn.consumerRequestBundler.addQueue(consumer.queue, BufferType.Free);
                } else {
                    auto producer = cast(Producer)worker;
                    assert(producer);
                    conn.producerRequestBundler.addQueue(producer.queue, BufferType.Filled);
                }
            } catch (ConnectionException) {
                // couldn't connect to the leader, readd this worker to brokerless workers to try again
                synchronized (m_mutex) {
                    m_brokerlessWorkers.insertBack(worker);
                }
            }
        }
    }

    private void checkWorkerExistence(IWorker worker, string name) {
        foreach (w; m_workers) {
            if (w.workerType == worker.workerType && w.topic == worker.topic && w.partition == worker.partition)
                throw new Exception(format("This client already has a %s for topic %s and partition %d",
                        name, w.topic, w.partition));
        }
    }

package: // functions below are used by the consumer and producer classes

    void addNewConsumer(Consumer consumer) {
        synchronized (m_mutex) {
            checkWorkerExistence(consumer, "consumer");
            m_workers.insertBack(consumer);
            m_brokerlessWorkers.insertBack(consumer);
            m_brokerlessWorkersEmpty.notify();
        }
    }

    void removeConsumer(Consumer consumer) {
        import std.algorithm;
        synchronized (m_mutex) {
            m_workers.remove(find(m_workers[], consumer));
        }
    }

    void addNewProducer(Producer producer) {
        synchronized (m_mutex) {
            checkWorkerExistence(producer, "producer");
            m_workers.insertBack(producer);
            m_brokerlessWorkers.insertBack(producer);
            m_brokerlessWorkersEmpty.notify();
        }
    }

    void addToBrokerless(Queue queue, bool notify = false) {
        m_brokerlessWorkers.insertBack(queue.worker);
        if (notify)
            m_brokerlessWorkersEmpty.notify();
    }

    void connectionLost(BrokerConnection conn) {
        synchronized (m_mutex) {
            synchronized (conn.consumerRequestBundler.mutex) {
                synchronized (conn.producerRequestBundler.mutex) {
                    foreach (pair; m_conns.byKeyValue) {
                        if (pair.value == conn) {
                            m_conns.remove(pair.key);
                            break;
                        }
                    }
                    foreach (q; &conn.consumerRequestBundler.queues) {
                        addToBrokerless(q);
                        synchronized (q.mutex) {
                            q.requestBundler = null;
                            q.requestPending = false;
                        }
                    }
                    foreach (q; &conn.producerRequestBundler.queues) {
                        addToBrokerless(q);
                        synchronized (q.mutex) {
                            q.requestBundler = null;
                            q.requestPending = false;
                        }
                    }
                    m_brokerlessWorkersEmpty.notify();
                }
            }
        }
    }
}

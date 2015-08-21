module kafkad.client;

import kafkad.connection;
import kafkad.protocol;
import core.time;
import std.exception;
import vibe.core.core;
import vibe.core.log;

/*
 * what is needed:
 * - broker list for bootstrap
 * - client id
 */

struct BrokerAddress {
    string host;
    ushort port;
}

class KafkaClient {
    private {
        BrokerAddress[] m_bootstrapBrokers;
        string m_clientId;
        BrokerConnection[int] m_conns;
        NetworkAddress[int] m_hostCache; // node id to netaddr cache
        Metadata m_metadata;
        bool m_connected;
    }
    
    import std.string, std.process;
    this(BrokerAddress[] bootstrapBrokers, string clientId = format("kafka-d-%d",thisProcessID) )
    {
        enforce(bootstrapBrokers.length);
        m_bootstrapBrokers = bootstrapBrokers;
        m_clientId = clientId;
        m_connected = false;
    }

    /// Bootstraps client into the cluser
    /// Params:
    /// retries = number of bootstrap retries, 0 = retry infinitely
    /// retryTimeout = time to wait between retries
    /// Returns: true if connected, false if all retries failed
    bool connect(size_t retries = 0, Duration retryTimeout = 1.seconds) {
        if (m_connected)
            return true;
        auto remainingRetries = retries;
        while (!retries || remainingRetries--) {
            foreach (brokerAddr; m_bootstrapBrokers) {
                try {
                    auto tcpConn = connectTCP(brokerAddr.host, brokerAddr.port);
                    auto host = tcpConn.remoteAddress;
                    auto conn = new BrokerConnection(this, tcpConn);
                    m_metadata = conn.getMetadata([]);
                    m_hostCache = null; // clear the cache

                    int bootstrapBrokerId = -1;
                    // look up this host in the metadata to obtain its node id
                    // also, fill the nodeid cache
                    foreach (ref b; m_metadata.brokers) {
                        enforce(b.port >= 0 && b.port <= ushort.max);
                        auto bhost = resolveHost(b.host);
                        bhost.port = cast(ushort)b.port;
                        if (bhost == host)
                            bootstrapBrokerId = b.id;
                        m_hostCache[b.id] = bhost;
                    }

                    enforce(bootstrapBrokerId >= 0);
                    conn.id = bootstrapBrokerId;
                    m_conns[conn.id] = conn;

                    debug {
                        logDebug("Broker list:");
                        foreach (ref b; m_metadata.brokers) {
                            logDebug("\tBroker ID: %d, host: %s, port: %d", b.id, b.host, b.port);
                        }
                        logDebug("Topic list:");
                        foreach (ref t; m_metadata.topics) {
                            logDebug("\tTopic: %s, partitions:", t.name);
                            foreach (ref p; t.partitions) {
                                logDebug("\t\tPartition: %d, Leader ID: %d, Replicas: %s, In sync replicas: %s",
                                    p.id, p.leader, p.replicas, p.isr);
                            }
                        }
                    }

                    m_connected = true;
                    return true;
                } catch (Exception e) {
                    continue;
                }
            }
            sleep(retryTimeout);
        }
        return false;
    }

    // TODO: handle stale metadata
    private auto getConn(int id) {
        auto pconn = id in m_conns;
        if (!pconn) {
            auto host = m_hostCache[id];
            auto tcpconn = connectTCP(host);
            auto conn = new BrokerConnection(this, tcpconn);
            m_conns[id] = conn;
            pconn = &conn;
        }
        return *pconn;
    }

    private auto topicsToConns(TopicPartitions[] topics) {
        // temp
    }

    @property auto clientId() { return m_clientId; }
    @property auto clientId(string v) { return m_clientId = v; }

    @property auto connected() { return m_connected; }
}

class KafkaConsumer {
    private {
        KafkaClient m_client;
        TopicPartitions[] m_topics;
    }
    this(KafkaClient client, TopicPartitions[] topics) {
        m_client = client;
        m_topics = topics;
    }

    /// Consumes message from the selected topics and partitions
    /// Returns: Ranges of ranges for topics, partitions, messages and message chunks
    TopicRange consume() {
        // TEMP HACK
        auto conn = m_client.m_conns.values[0]; // FIXME
        return conn.getTopicRange(m_topics);
    }
}

enum KafkaCompression {
    None = 0,
    GZIP = 1,
    Snappy = 2
}

struct PartitionOffset {
    int partition;
    long offset;
}

struct TopicPartitions {
    string topic;
    PartitionOffset[] partitions;
}

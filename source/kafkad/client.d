module kafkad.client;

import kafkad.connection;
import kafkad.protocol;
import kafkad.exception;
import core.time;
import std.exception;
import vibe.core.core;
import vibe.core.log;
public import kafkad.config;

/*
 * what is needed:
 * - broker list for bootstrap
 * - client id
 */

struct BrokerAddress {
    string host;
    ushort port;
}

class Client {
    private {
        Configuration m_config;
        BrokerAddress[] m_bootstrapBrokers;
        string m_clientId;
        BrokerConnection[NetworkAddress] m_conns;
        NetworkAddress[int] m_hostCache; // broker id to NetworkAddress cache
        Metadata m_metadata;
        bool m_connected;
    }
    
    import std.string, std.process;
    this(BrokerAddress[] bootstrapBrokers, string clientId = format("kafka-d-%d",thisProcessID),
        Configuration config = Configuration())
    {
        m_config = config;
        enforce(bootstrapBrokers.length);
        m_bootstrapBrokers = bootstrapBrokers;
        m_clientId = clientId;
        m_connected = false;
    }

    /// Bootstraps client into the cluser
    /// Returns: true if connected, false if all retries failed
    bool connect() {
        if (m_connected)
            return true;
        return refreshMetadata();
    }

    private bool refreshMetadata() {
        auto remainingRetries = m_config.metadataRefreshRetryCount;
        while (!m_config.metadataRefreshRetryCount || remainingRetries--) {
            foreach (brokerAddr; m_bootstrapBrokers) {
                try {
                    auto conn = getConn(brokerAddr);
                    auto host = conn.addr;
                    m_metadata = conn.getMetadata([]);
                    enforce(m_metadata.brokers.length, "Empty metadata, this may indicate there are no defined topics in the cluster");
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

                    enforce(bootstrapBrokerId >= 0);
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
                    return true;
                } catch (ConnectionException) {
                    continue;
                }
            }
            sleep(m_config.metadataRefreshRetryTimeout.msecs);
        }
        return false;
    }

    private NetworkAddress resolveBrokerAddr(BrokerAddress brokerAddr) {
        auto netAddr = resolveHost(brokerAddr.host).rethrow!ConnectionException("Could not resolve host " ~ brokerAddr.host);
        netAddr.port = brokerAddr.port; 
        return netAddr;
    }

    private BrokerConnection getConn(BrokerAddress brokerAddr) {
        auto netAddr = resolveBrokerAddr(brokerAddr);
        return getConn(netAddr);
    }

    private BrokerConnection getConn(NetworkAddress netAddr) {
        auto pconn = netAddr in m_conns;
        if (!pconn) {
            auto tcpConn = connectTCP(netAddr).rethrow!ConnectionException("TCP connect to address " ~ netAddr.toString() ~ " failed");
            auto conn = new BrokerConnection(this, tcpConn);
            m_conns[netAddr] = conn;
            pconn = &conn;
        }
        return *pconn;
    }

    // TODO: handle stale metadata
    private auto getConn(int id) {
        assert(id in m_hostCache);
        auto netAddr = m_hostCache[id];
        auto conn = getConn(netAddr);
        conn.id = id;
        return conn;
    }

    private auto topicsToConns(TopicPartitions[] topics) {
        // temp
    }

    @property auto clientId() { return m_clientId; }
    @property auto clientId(string v) { return m_clientId = v; }

    @property ref const(Configuration) config() { return m_config; }

    @property auto connected() { return m_connected; }
}

class Consumer {
    private {
        Client m_client;
        TopicPartitions[] m_topics;
    }
    this(Client client, TopicPartitions[] topics) {
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

enum Compression {
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

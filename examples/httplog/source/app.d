import std.stdio;
import vibe.d;
import kafkad.client;

shared static this()
{
    debug setLogLevel(LogLevel.debug_);

    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    
    listenHTTP(settings, &handleRequest);

    producerTid = runTask({
        Configuration config;
        // adjust config's properties if necessary
        config.metadataRefreshRetryCount = 0;
        
        Client client = new Client([BrokerAddress("127.0.0.1", 9092)], "kafka-d", config);
        Producer producer = new Producer(client, "httplog", 0);
        for (;;) {
            receive((string s) { 
                try {
                    producer.pushMessage(null, cast(ubyte[])s);
                } catch (Exception ex) {
                    // this producer is now invalid and removed from the client, create another one to continue
                    producer = new Producer(client, "httplog", 0);
                }
            });
        }
    });

    setMaxMailboxSize(producerTid, 100, OnCrowding.throwException);
}

shared Tid producerTid;

void handleRequest(HTTPServerRequest req,
    HTTPServerResponse res)
{
    if (req.path == "/")
        res.writeBody("Hello, World!", "text/plain");

    try {
        send(producerTid, req.path);
    } catch (MailboxFull) {
        // message queue is full, this may indicate that producer is blocking, no connection, etc.
        logInfo("Couldn't log to kafka");
    }
}
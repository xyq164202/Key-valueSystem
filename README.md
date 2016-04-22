# Key-valueSystem
In this project, we use the Amazon AWS cloud to evaluate various distributed key/value storage systems. we evaluate Amazon DynamoDB, MongoDB, Redis, Cassandra and my own P2P system.
On each instance/node, a client-server pair is deployed. Test workload is a set of key-value pairs where the key is 10 bytes and value is 90 bytes. Clients sequentially send all of the key-value pairs through a client API for insert, then lookup, and then remove. My keys was randomly generated, which will produce an All-to-All communication pattern, with the same number of servers and clients.
The metrics I  measure and report are:
• Latency: Latency presents the time per operation (insert/lookup/remove) taken from a request to be submitted from a client to a response to be received by the client, measured in milliseconds (ms). Note that the latency consists of round trip network communication, system processing, and storage access time
• Throughput: The number of operations (insert/lookup/remove) the system can handle over some period of time, measured in Kilo Ops/s

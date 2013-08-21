dynamite
=========

## Overview
Dynamite is an in-memory, distributed NoSQL database engine, written in Scala.

## Network Protocol
Data is transmitted between clients and the main controller node--then, between the controller node and storage servers--using TCP/IP sockets.

The controller and client communicate by default on port 4343. Servers also communicate with the controller on port 4343, but communicate with other servers during key migration on user-defined ports, entered as arguments in the command line.

## Controller
The controller receives requests from clients and forwards those requests to appropriate servers using consistent hashing.

When a *client* initially connects to the network, the first Akka actor (acceptor) creates a new Client object, which stores that client's corresponsing socket, InputStream, PrintStream, and identifying name. The Client object is then added to an ArrayBuffer, which is continuously iterated over by a second Akka actor (clientCommunicator). The clientCommunicator processes incoming messages from clients and sends responses from the database.

When a *server* initially connects to the network, the first Akka actor (acceptor) creates a Server object, which stores that server's corresponding socket, InputStream, PrintStream, and port number. The Server object is then added to a ConcurrentLinkedQueue, which is continuously monitored by a third Akka actor (serverAdder). The serverAdder processes new servers one by one, as they are added to the network, and initiates key-value pair migrations in the appropriate order.

## Next Steps
1. Improve load balancing by hashing servers to the keyContinuum multiple times.
2. If an added server goes offline, and the controller attempts to set a key at that server, the controller should send the KVP to another server temporarily. When the server comes back online, the secondary server should migrate all temporarily KVPs accordingly.
3. Add replicant servers.
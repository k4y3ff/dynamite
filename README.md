dynamite
=========

## To Do
1. If an added server goes offline, and the controller attempts to INITIALLY set a key at that server, the controller should send the KVP to another server temporarily. That server should ping the other server until it comes back online, then migrate all temporarily KVPs accordingly.
2. Fix hashRing.migrate (see comment).
3. sbt doesn't allow backspaces in the terminal? Probably should write a proper client....

# Fault tolerance goals
- When a server is disconnected from the network), its KVPs should be inaccessible, and any KVPs hashed to that server's section of the ring should be sent to another server. When the server comes back reconnects to the network, keys hashed to the "backup" server should be migrated accordingly.

## By the Way
- Everything is strings, all of the time, always... for now. The input? Strings. The keys? Strings. The values? Strings. *Literally everything is a string.* I'll generalize later.
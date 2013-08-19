dynamite
=========

## To Do
1. Split many, many things into separate functions.
2. If an added server goes offline, and the controller attempts to set a key at that server, the controller should send the KVP to another server temporarily. When the server comes back online, the secondary server should migrate all temporarily KVPs accordingly.
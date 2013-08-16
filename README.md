dynamite
=========

## To Do
1. Figure out how to pass in port number for server as an argument in the sbt run command prompt.
2. Split many, many things into separate functions.
3. If an added server goes offline, and the controller attempts to set a key at that server, the controller should send the KVP to another server temporarily. When the server comes back online, the secondary server should migrate all temporarily KVPs accordingly.
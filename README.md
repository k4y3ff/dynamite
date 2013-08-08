dynamite
=========

## By the Way
- Everything is strings, all of the time, always... for now. The input? Strings. The keys? Strings. The values? Strings. *Literally everything is a string.* I'll generalize later.

## To Do
1. sbt doesn't allow backspaces in the terminal? Probably should write a proper client....
2. Add error message so that if a user tries to set the value for a key that already exists, it will prompt the user to update, instead.
3. Add an update command.
4. Space the client printouts properly.
5. Show message on server terminal window when a key is deleted.
6. Show message on server terminal window when a key is changed.
7. Show message in controller terminal window when a KVP is sent to a certain server.
8. The status message doesn't have proper ranges (i.e. (-∞, ∞)).
	- What are the max and min possible hash values generated by MurmurHash 3?
9. Fix concurrency issues for the controller/servers.
10. Fault tolerance!

# App goals
- Write a task list (or something) that uses the database as its backend.
	1. Write it with Python/Flask.
	2. Write it with Scala/Play.

# Fault tolerance goals
- When a server is killed (or removed from the network), its KVPs should be inaccessible, and any KVPs hashed to that server's section of the ring should be sent to another server. When the server comes back online (or disconnects from the network), keys hashed to the "backup" server should be migrated accordingly.
	- If I go the server-killing route (vs. removing servers from the network), I need to make the datastore persistent.
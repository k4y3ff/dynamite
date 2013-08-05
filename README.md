dynamite
=========

## To Do
1. Write get function for coordinator.
2. Fix error: https://gist.github.com/kpfell/e72a037403bfdd725560
3. Write replicant servers.

## Ports
- Client
	- Incoming: --
	- Outgoing: 4343
- Controller
	- Incoming: 4343
	- Outgoing: ++
- Server 0
	- Incoming: 4000
	- Outgoing: 4343
- Server 1
	- Incoming: 4010
	- Outgoing: 4343
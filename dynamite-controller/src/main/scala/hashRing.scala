import collection.mutable

object hashRing {
	
	case class Server(serverID:Integer, port:Integer, position:Double)

	var locations : Integer = Set()
	val serverLocations = collection.mutable.Map[Integer, String]() // Map of locations on the ring to servers
	val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// Adds a new server node to the hash ring
	def addServer(serverID:Integer, port:Integer): Unit = {
		val random = new scala.util.Random

		val serverID = servers.length // Labels the new server with a unique ID number
		var position = random.nextDouble() // Generates a random (Double) position on the hash ring from (0, 1)

		// So long as the position is not unique (i.e. is occupied by another server), generates a new position
		while locations(position) == true { 
			position = random.nextDouble()
		}

		servers += Server(serverID, port, position) // Adds the server to the list of servers
		serverLocations(position) = serverID // Adds the server ID to the Map of ring positions to IDs
	}

	// Adds a new key-value pair to the hash ring
	def addPair(key:String, value:String) {

	}

}
import collection.mutable

/*
/ NOTE:
/ This hash ring assumes that you distribute ALL servers around the ring first, THEN distribute your key-value pairs.
/ If you add a new server after having distributed key-value pairs, the distribution will not be "even."
/ tl;dr Add ALL of your servers before you anything else, or things will hit the fan.
*/

object hashRing {
	
	case class Server(serverID:Integer, port:Integer, position:Double)

	val random = new scala.util.Random
	
	val kvLocations = collection.mutable.Map[String, Integer]
	var locations : Integer = Set()
	val serverLocations = collection.mutable.Map[Integer, String]() // Map of locations on the ring to servers
	val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// Adds a new server node to the hash ring
	def addServer(serverID:Integer, port:Integer): Unit = {
		val serverID = servers.length // Labels the new server with a unique ID number
		var serverPosition = random.nextDouble() // Generates a random (Double) position on the hash ring from (0, 1)

		// So long as the server position is not unique (i.e. is occupied by another server), generates a new position
		while locations(serverPosition) == true { 
			serverPosition = random.nextDouble()
		}

		servers += Server(serverID, port, serverPosition) // Adds the server to the list of servers
		serverLocations(serverPosition) = serverID // Adds the server ID to the Map of ring positions to IDs
	}

	// Adds a new key-value pair to the hash ring
	def addPair(key:String, value:String) {
		val kvPosition = random.nextDouble()

		while locations(kvPosition) == true {
			kvPosition = random.nextDouble()
		}





		/*
		/
		/ WRITE CODE TO ADD KEY TO SERVER'S MAP HERE
		/
		*/
	}

}
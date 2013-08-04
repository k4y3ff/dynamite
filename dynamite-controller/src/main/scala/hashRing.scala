package controller

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import collection.mutable

import java.util.TreeMap
import scala.collection.JavaConversions._
import java.net.Socket
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

/*
/ NOTE:
/ This hash ring assumes that you distribute ALL servers around the ring first, THEN distribute your key-value pairs.
/ If you add a new server after having distributed key-value pairs, the distribution will not be "even."
/ tl;dr Add ALL of your servers before you anything else, or things will hit the fan.
/
/ ALSO:
/ This coordinator/hash ring very likely doesn't work for multiple clients. :(
*/

object hashRing {

	val host = "localhost"

	
	case class Server(port:Int, position:Double)

	val random = new scala.util.Random // Instantiation for random number generator
	val highestRandomValue = 1 // Highest random value the can be generated

	val locations = Set.empty[Double] // Set that contains locations of servers on the hash ring; allows for fast lookup
	val keyContinuum = new TreeMap[Double, String] // Ordered map of locations -> keys on the hash ring; underlying structure is red-black tree
	val serverContinuum = new TreeMap[Double, Server] // Ordered map of locations -> servers on the hash ring; underlying structure is red-black tree
	//val serverLocations = collection.mutable.Map[Double, Int]() // Map of locations on the ring to servers
	val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// TO DO: 
	// 1. Fix so that adding a new server after KVPs have been distributed *doesn't* break the database.
	//
	// Adds a new server node to the hash ring
	def addServerToRing(port:String): Boolean = {
		var serverPosition = random.nextDouble() // Generates a random (Double) position on the hash ring from (0, 1)

		// So long as the server position is not unique (i.e. is occupied by another server), generates a new position
		while ((locations contains serverPosition) == true) { 
			serverPosition = random.nextDouble()
		}

		val server = Server(port.toInt, serverPosition)
		serverContinuum(serverPosition) = server

		migrateKVPs(serverPosition)

		true
	}

	// Generates a random position on the hash ring for a key-value pair, iterates over the Map of server locations, 
	// and returns the ID number of the server who is closest (in a clockwise direction) to the key-value pair.
	def addPairToRing(key:String, value:String): Boolean = {
		val kvPosition = random.nextDouble() // Generates a random position on the hash ring for the key-value pair
		var nearestServerLocation = serverContinuum.ceilingKey(kvPosition)
		if (nearestServerLocation == null) nearestServerLocation = serverContinuum.firstKey

		val nearestServer = serverContinuum(nearestServerLocation)
		
		val port = nearestServer.port
		val sock = new Socket(host, port)
		val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
		val ps = new PrintStream(sock.getOutputStream())

		ps.println("set " + key + " " + value)
		val output = is.readLine // READLINE IS A BLOCKING CALL. THIS IS BAD, BAD CODE.
		sock.close()

		keyContinuum(kvPosition) = key

		true
	}

	// Returns list of servers and their locations on the ring
	def listServers(): TreeMap[Double, Server] = serverContinuum
	
	def migrateKVPs(newServerPosition:Double): Unit = {
		if(serverContinuum.size > 0) {

			// Determine location of previous node
			var previousServerPosition = serverContinuum.lowerKey(newServerPosition)

			// Determine location of next node
			var nextServerPosition = serverContinuum.higherKey(newServerPosition)

			// Case 1: Location of previous location == null:
			if (previousServerPosition == null) {
				// Create a submap of all keys between the position of the last server (exclusive) and the end of the keyContinuum (inclusive)
				val migratedKeys1 = keyContinuum.subMap(serverContinuum.lastKey, false, highestRandomValue, true)
				// Create a submap of all keys between the beginning of the keyContinuum (inclusive) and the position of the new server (inclusive)
				val migratedKeys2 = keyContinuum.subMap(0, false, newServerPosition, true)

				// Open connection to new server
				val newServerPort = serverContinuum(newServerPosition).port
				val newServerSock = new Socket(host, newServerPort)
				val newServerPS = new PrintStream(newServerSock.getOutputStream())

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(nextServerPosition).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				// Iterate over first submap, adding each KVP to new server, then removing from old server
				migratedKeys1.foreach(pair => {
					val key = pair._1
					val value = oldServerPS.println("get " + pair._1)
					newServerPS.println("set " + key + " " + value)
					oldServerPS.println("delete " + key)
				})

				// Iterate over second submap, adding each KVP to new server, then removing from old server
				migratedKeys2.foreach(pair => {
					val key = pair._1
					val value = oldServerPS.println("get " + pair._1)
					newServerPS.println("set " + key + " " + value)
					oldServerPS.println("delete " + key)
				})

			}

			// Case 2: Location of next location == null:
			else if (nextServerPosition == null) {

				// Create a submap of all keys between the position of the preceding server and the location of the new server
				val migratedKeys = keyContinuum.subMap(previousServerPosition, false, newServerPosition, true)

				// Open connection to new server
				val newServerPort = serverContinuum(newServerPosition).port
				val newServerSock = new Socket(host, newServerPort)
				val newServerPS = new PrintStream(newServerSock.getOutputStream())

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(serverContinuum.firstKey).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				// Iterate over submap, adding each KVP to new server, then removing from oldserver
				migratedKeys.foreach(pair => {
					val key = pair._1
					val value = oldServerPS.println("get " + pair._1)
					newServerPS.println("set " + key + " " + value)
					oldServerPS.println("delete " + key)
				})
			}

			// Case 3
			else {
				val migratedKeys = keyContinuum.subMap(previousServerPosition, false, newServerPosition, true)

				// Open connection to new server
				val newServerPort = serverContinuum(newServerPosition).port
				val newServerSock = new Socket(host, newServerPort)
				val newServerPS = new PrintStream(newServerSock.getOutputStream())

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(serverContinuum.firstKey).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				// Iterate over submap, adding each KVP to new server, then removing from oldserver
				migratedKeys.foreach(pair => {
					val key = pair._1
					val value = oldServerPS.println("get " + pair._1)
					newServerPS.println("set " + key + " " + value)
					oldServerPS.println("delete " + key)
				})
			}
		}
	}
	

}
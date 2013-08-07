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

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.hashing.MurmurHash3

/*
/ NOTE:
/ This coordinator/hash ring doesn't work for multiple clients yet. :(
*/

object hashRing {

	val host = "localhost"

	case class Server(port:Int, position:Integer)

	val seed = 1234567890 // Manually set seed value used to hash strings with MurmurHash 3
	
	val keyContinuum = new TreeMap[Integer, String] // Ordered map of locations -> keys on the hash ring; underlying structure is red-black tree
	val serverContinuum = new TreeMap[Integer, Server] // Ordered map of locations -> servers on the hash ring; underlying structure is red-black tree
	val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// Adds a new server node to the hash ring
	def addServerToRing(port:String): Boolean = {
		var serverPosition = MurmurHash3.stringHash(port, seed)

		// So long as the server position is not unique (i.e. is occupied by another server), generates a new position
		while ((serverContinuum containsKey serverPosition) == true) { 
			serverPosition = MurmurHash3.stringHash(port, seed)
		}

		val server = Server(port.toInt, serverPosition)
		serverContinuum(serverPosition) = server

		///////////////////////////////////////////////////////////////////////////////////////////////////////////
		if (serverContinuum contains serverPosition) println("Added server to position " + serverPosition + ".") // Prints to terminal for debugging
		///////////////////////////////////////////////////////////////////////////////////////////////////////////

		migrateKVPs(serverPosition)

		true
	}

	// Generates a position on the hash ring for a key-value pair, iterates over the Map of server locations, 
	// and returns the ID number of the server who is closest (in a clockwise direction) to the key-value pair.
	def addPairToRing(key:String, value:String): Boolean = {

		val kvPosition = MurmurHash3.stringHash(key, seed) // Generates a position on the hash ring for the key-value pair
		
		//////////////////////////////////////////////////////////////////////////
		println("Generated hash value " + kvPosition + " for key " + key + ".") // Prints to terminal for debugging
		//////////////////////////////////////////////////////////////////////////

		var nearestServerLocation = serverContinuum.ceilingKey(kvPosition)
		if (nearestServerLocation == null) nearestServerLocation = serverContinuum.firstKey

		val nearestServer = serverContinuum(nearestServerLocation)

		val port = nearestServer.port
		val sock = new Socket(host, port)
		val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
		val ps = new PrintStream(sock.getOutputStream())

		ps.println("get " + key)
		var output = is.readLine // READLINE IS A BLOCKING CALL. THIS IS BAD, BAD, BAD.

		if (output != "false") return false // This is problematic, because someone might want to store the string "false"

		ps.println("set " + key + " " + value)
		output = is.readLine // READLINE IS A BLOCKING CALL. THIS IS BAD, BAD, BAD.
		sock.close()

		keyContinuum(kvPosition) = key

		true
	}

	def getValue(key:String): String = {
		val keyPosition = MurmurHash3.stringHash(key, seed)
		var serverPosition = serverContinuum.higherKey(keyPosition)
		if (serverPosition == null) serverPosition = serverContinuum.firstKey

		val serverPort = serverContinuum(serverPosition).port
		val serverSock = new Socket(host, serverPort)
		val serverPS = new PrintStream(serverSock.getOutputStream())
		val is = new BufferedReader(new InputStreamReader(serverSock.getInputStream()))

		serverPS.println("get " + key)
		val value = is.readLine // READLINE IS A BLOCKING CALL. THIS IS BAD, BAD, BAD.
		serverSock.close()

		value
	}

	// Returns list of servers and their locations on the ring
	def listServers(): TreeMap[Integer, Server] = serverContinuum
	
	def migrateKVPs(newServerPosition:Integer): Unit = {
		if(serverContinuum.size > 1) {

			///////////////////////////////////////////////////////
			println(serverContinuum.size + " servers detected.") // Prints to terminal for debugging
			///////////////////////////////////////////////////////

			// Determine location of previous node
			var previousServerPosition = serverContinuum.lowerKey(newServerPosition)

			// Determine location of next node
			var nextServerPosition = serverContinuum.higherKey(newServerPosition)

			/*
			/ There's almost certainly a way to write the hash ring without a case-by-case structure, but 
			/ at the moment, I'm not sure how.
			/ 
			/ Also, key migration is really terrible at the moment. I'm currently working on migrating directly
			/ between servers.
			*/

			// Case 1: Location of previous location == null:
			if (previousServerPosition == null) { // SHOULD BE USING PATTERN MATCHING!

				// Get port of new server
				val newServerPort = serverContinuum(newServerPosition).port

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(nextServerPosition).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerIS = new BufferedReader(new InputStreamReader(oldServerSock.getInputStream()))				
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				// Tell the old server to migrate the keys hashed between the "last" server on the ring and the "end" of the ring to the new server
				oldServerPS.println("migrate " + serverContinuum.lastKey + " " + "end" + " " + seed + " " + newServerPort)
				// Tell the old server to migrate the keys hashed between the "beginning" of the ring and the new server's location to the new server
				oldServerPS.println("migrate " + "beginning" + " " + newServerPosition + seed + newServerPort)

				// newServerSock.close()
				oldServerSock.close()
			}

			// Case 2: Location of next location == null:
			else if (nextServerPosition == null) { // SHOULD BE USING PATTERN MATCHING!

				val newServerPort = serverContinuum(newServerPosition).port

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(serverContinuum.firstKey).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerIS = new BufferedReader(new InputStreamReader(oldServerSock.getInputStream()))				
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				// Tell the old server to migrate the keys hashed between the previous server on the ring and the new server on the ring to the new server
				oldServerPS.println("migrate " + previousServerPosition + " " + newServerPosition + " " + seed + " " + newServerPort)

				oldServerSock.close()
			}

			// Case 3
			else { // SHOULD BE USING PATTERN MATCHING!
				val migratedKeys = keyContinuum.subMap(previousServerPosition, false, newServerPosition, true)

				// Open connection to old server that the keys will be moved from
				val oldServerPort = serverContinuum(serverContinuum.firstKey).port
				val oldServerSock = new Socket(host, oldServerPort)
				val oldServerIS = new BufferedReader(new InputStreamReader(oldServerSock.getInputStream()))
				val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

				oldServerPS.println("migrate " + previousServerPosition + " " + newServerPosition + " " + seed + " " + newServerPosition)

				oldServerSock.close()
			}
		}
	}
	

}
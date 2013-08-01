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

	
	case class Server(serverID:Int, port:Int, position:Double)

	val random = new scala.util.Random

	val locations = Set.empty[Double]
	val continuum = new TreeMap[Double, Server]()
	val serverLocations = collection.mutable.Map[Double, Int]() // Map of locations on the ring to servers
	val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// TO DO: 
	// 1. Fix so that adding a new server after KVPs have been distributed *doesn't* break the database.
	//
	// Adds a new server node to the hash ring
	def addServerToRing(port:String): Boolean = {
		val serverID = servers.length // Labels the new server with a unique ID number
		var serverPosition = random.nextDouble() // Generates a random (Double) position on the hash ring from (0, 1)

		// So long as the server position is not unique (i.e. is occupied by another server), generates a new position
		while ((locations contains serverPosition) == true) { 
			serverPosition = random.nextDouble()
		}

		val server = Server(serverID.toInt, port.toInt, serverPosition)
		continuum(serverPosition) = server

		true
		
		// OLD CODE
		// servers += Server(serverID, port, serverPosition) // Adds the server to the list of servers
		// continuum(serverPosition) = serverID // Adds the server ID to the Map of ring positions to IDs
	}

	// Generates a random position on the hash ring for a key-value pair, iterates over the Map of server locations, 
	// and returns the ID number of the server who is closest (in a clockwise direction) to the key-value pair.
	def addPairToRing(key:String, value:String): Boolean = {
		val kvLocation = random.nextDouble() // Generates a random position on the hash ring for the key-value pair
		var nearestServerLocation = continuum.ceilingKey(kvLocation)
		if (nearestServerLocation == null) nearestServerLocation = continuum.firstKey

		val nearestServer = continuum(nearestServerLocation)
		
		val port = nearestServer.port
		val sock = new Socket(host, port)
		val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
		val ps = new PrintStream(sock.getOutputStream())

		ps.println("set " + key + " " + value)
		val output = is.readLine // READLINE IS A BLOCKING CALL. THIS IS BAD, BAD CODE.
		sock.close()

		true
	}
	

}
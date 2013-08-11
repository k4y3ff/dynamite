package controller

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import collection.mutable

import java.util.TreeMap
import scala.collection.JavaConversions._
import java.net.Socket
import java.net.InetSocketAddress
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.hashing.MurmurHash3
import scala.util.{Try, Success, Failure}

/*
/ NOTE:
/ This coordinator/hash ring doesn't work for multiple clients yet. :(
*/

object hashRing {

	val host = "localhost"

	case class Server(port:Int, position:Int, name:String)

	val seed = 1234567890 // Manually set seed value used to hash strings with MurmurHash 3
	
	val serverContinuum = new TreeMap[Integer, Server] // Ordered map of locations -> servers on the hash ring; underlying structure is red-black tree
	//val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] // Can I just make this a regular array?

	// Adds a new server node to the hash ring
	def addServerToRing(port:String): Boolean = {
		val serverName = "server" + serverContinuum.size.toString

		// Verify that the server socket is open before adding the server to the network
		val serverSock = new Socket()

		try {
    		serverSock.connect(new InetSocketAddress(host, port.toInt), 5000)
		}
		catch {
			case ex: java.net.ConnectException => {
				return false // Need to send a PROPER error message back
			}
		}

		var serverPosition = MurmurHash3.stringHash(port, seed)

		// So long as the server position is not unique (i.e. is occupied by another server), generates a new position
		while ((serverContinuum containsKey serverPosition) == true) { 
			serverPosition = MurmurHash3.stringHash(port, seed)
		}

		val server = Server(port.toInt, serverPosition, serverName)
		serverContinuum(serverPosition) = server

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		if (serverContinuum contains serverPosition) println("Added server at port " + port + " at hash value " + serverPosition + ".") // Prints to terminal for debugging
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		migrateKVPs(serverPosition) // NEED TO CLOSE THE SERVERSOCKET!!!

		true
	}

	// Generates a position on the hash ring for a key-value pair, iterates over the Map of server locations, 
	// and returns the ID number of the server who is closest (in a clockwise direction) to the key-value pair.
	def addPairToRing(key:String, value:String): Boolean = {

		val kvPosition = MurmurHash3.stringHash(key, seed) // Generates a position on the hash ring for the key-value pair

		serverContinuum.size match {
			case 0 => {
				println("No server available to save KVP '" + key + "'.")
				false
			}

			case _ => {
				println("Generated hash value " + kvPosition + " for key " + key + "'.")

				val nearestServerLocation = Option(serverContinuum.ceilingKey(kvPosition)).getOrElse(serverContinuum.firstKey)
				val nearestServer = serverContinuum(nearestServerLocation)

				val port = nearestServer.port
				val sock = new Socket()

				try {
    				sock.connect(new InetSocketAddress(host, port), 5000)
				}
				catch {
					case ex: java.lang.NullPointerException => return false
					case ex: java.net.ConnectException => return false // Need to send a PROPER error message back
				}

				val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
				val ps = new PrintStream(sock.getOutputStream())

				// Double-checks that the key does not already exist in the database, with some assigned value
				ps.println("get " + key)
				var output = is.readLine // Blocking call

				output match {
					case "false" => {
						ps.println("set " + key + " " + value)
						output = is.readLine // Blocking call

						sock.close()

						if (output == "true") println("KVP '" + key + "' assigned to " + nearestServer.name + " at port " + port + ".")

						true
					}
					
					case _ => false // This is problematic, because someone might want to store the string 'false'.
				}
			}
		}
	}

	def deleteKVP(key: String): String = {
		
		val kvpPosition = MurmurHash3.stringHash(key, seed)
		val serverPosition = Option(serverContinuum.higherKey(kvpPosition)).getOrElse(serverContinuum.firstKey)
				
		val serverPort = serverContinuum(serverPosition).port
		val serverSock = new Socket()

		Try(serverSock.connect(new InetSocketAddress(host, serverPort), 5000)) match {
			case Success(_) => delete()
			case Failure(_) => "false"
		}

		// try {
  //   		serverSock.connect(new InetSocketAddress(host, serverPort), 5000)
		// }
		// catch {
		// 	case ex: java.lang.NullPointerException => return "false"
		// 	case ex: java.net.ConnectException => return "false" // Need to send a PROPER error message back
		// }

		def delete(): String = {
			val serverPS = new PrintStream(serverSock.getOutputStream())
			val serverIS = new BufferedReader(new InputStreamReader(serverSock.getInputStream()))

			serverPS.println("delete " + key)
			val confirmation = serverIS.readLine // Blocking call
			serverSock.close()

			confirmation
		}

		// val serverPS = new PrintStream(serverSock.getOutputStream())
		// val serverIS = new BufferedReader(new InputStreamReader(serverSock.getInputStream()))

		// serverPS.println("delete " + key)
		// val confirmation = serverIS.readLine // Blocking call
		// serverSock.close()

		// confirmation
	}

	def getValue(key:String): String = {
		val keyPosition = MurmurHash3.stringHash(key, seed)

		val serverPosition = Option(serverContinuum.higherKey(keyPosition)).getOrElse(serverContinuum.firstKey)

		val serverPort = serverContinuum(serverPosition).port
		
		val serverSock = new Socket

		try {
    		serverSock.connect(new InetSocketAddress(host, serverPort), 5000)
		}
		catch {
			case ex: java.lang.NullPointerException => return "false" // NEED TO RETURN AN ACTUAL VALUE THAT INDICATES FAILURE
			case ex: java.net.ConnectException => return "false" // Need to send a PROPER error message back

		}
		val serverPS = new PrintStream(serverSock.getOutputStream())
		val is = new BufferedReader(new InputStreamReader(serverSock.getInputStream()))

		serverPS.println("get " + key)
		val value = is.readLine // Blocking call
		serverSock.close()

		value
	}

	// Returns list of servers and their locations on the ring
	def listServers(): TreeMap[Integer, Server] = serverContinuum
	
	def migrateKVPs(newServerPosition:Integer): Unit = { // NEEDS TO RETURN A BOOLEAN BEFORE ADDING TIMEOUT MAX, TRY-CATCH, ETC.
		if(serverContinuum.size > 1) {

			/*
			/ There's almost certainly a way to write the hash ring without a case-by-case structure, but 
			/ at the moment, I'm not sure how.
			*/

			// Determine location of previous node
			val previousServerPosition = Option(serverContinuum.lowerKey(newServerPosition))

			// Determine location of next node
			val nextServerPosition = Option(serverContinuum.higherKey(newServerPosition))

			previousServerPosition match {
				
				// Case 1: The new server is the "first" server on the hash ring, clockwise from 12:00
				case None => {
					
					// Get port of new server
					val newServerPort = serverContinuum(newServerPosition).port

					// Open connection to old server that the keys will be moved from
					val oldServerPort = serverContinuum(nextServerPosition.get).port
					val oldServerSock = new Socket(host, oldServerPort)
					val oldServerIS = new BufferedReader(new InputStreamReader(oldServerSock.getInputStream()))
					val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

					// Migrate the keys hashed between the "last" server on the ring and the "end" of the ring from the old server to the new server
					oldServerPS.println("migrate " + serverContinuum.lastKey + " " + "end" + " " + seed + " " + newServerPort)
					// Migrate the keys hashed between the "beginning" of the ring and the new server's location from the old server to the new server
					oldServerPS.println("migrate " + "beginning" + " " + newServerPosition + seed + newServerPort)

					oldServerSock.close()
				}

				case Some(previousServerPosition) => {

					nextServerPosition match {

						// Case 2: The new server is the "last" server on the hash ring, clockwise from 12:00
						case None => {

							val newServerPort = serverContinuum(newServerPosition).port

							// Open connection to old server that the keys will be moved from
							val oldServerPort = serverContinuum(serverContinuum.firstKey).port
							val oldServerSock = new Socket(host, oldServerPort)
							val oldServerIS = new BufferedReader(new InputStreamReader(oldServerSock.getInputStream()))
							val oldServerPS = new PrintStream(oldServerSock.getOutputStream())

							// Migrate the keys hashed between the previous server on the ring and the new server on the ring from the old server to the new server
							oldServerPS.println("migrate " + previousServerPosition + " " + newServerPosition + " " + seed + " " + newServerPort)

							oldServerSock.close()
						}

						// Case 3: The new server is neither the "first" nor the "last" server on the hash ring, clockwise from 12:00
						case Some(nextServerPosition) => {

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
		}
	}

	def status(): String = {
		var statusMessage = "\nStatus\n====================\n"

		if (serverContinuum.size < 1) {
			statusMessage += "No servers connected.\n\n"
			return statusMessage
		}

		for ((location, server) <- serverContinuum) {
			statusMessage += server.name + "\n--------------------\nPort: " + server.port + "\nHash Location: " + location + "\n"

			val serverPort = server.port

			val serverSock = new Socket()

			try {
				serverSock.connect(new InetSocketAddress(host, serverPort), 5000)
				val serverIS = new BufferedReader(new InputStreamReader(serverSock.getInputStream()))
				val serverPS = new PrintStream(serverSock.getOutputStream())

				serverPS.println("countKVPs")
				val kvpCount = serverIS.readLine // Blocking call

				serverSock.close()

				statusMessage += "KVP Count: " + kvpCount + "\n"

				val kvpLowValue = Option(serverContinuum.lowerKey(location))
				
				kvpLowValue match {
					case None => {
						val kvpRange = "(-∞, " + location + "]" + " U (" + serverContinuum.lastKey + ", " + "∞)"
						statusMessage += "KVP Range: " + kvpRange + "\n\n"
					}
					case Some(kvpLowValue) => {
						val kvpRange = "(" + kvpLowValue + ", " + location + "]"
						statusMessage += "KVP Range: " + kvpRange + "\n\n"
					}
				}
			}
			catch {
				case ex: java.lang.NullPointerException => statusMessage += "OFFLINE\n\n"
				case ex: java.net.ConnectException => statusMessage += "OFFLINE\n\n"
			}

		}

		statusMessage
	}
	

}
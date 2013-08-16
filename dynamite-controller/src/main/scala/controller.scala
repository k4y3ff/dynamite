import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import collection.mutable

import java.net.{ ServerSocket, Socket }
import java.io.{ BufferedReader, InputStreamReader, PrintStream }
import java.util.concurrent.ConcurrentLinkedQueue

// import ExecutionContext.Implicits.global
import java.util.TreeMap
import scala.collection.JavaConversions._
import scala.concurrent._
import scala.util.hashing.MurmurHash3
import scala.util.{ Try, Success, Failure }

object controller {

	val host = "localhost"
	val controllerPort = 4343

	val seed = 1234567890 // Seed for MurmurHash 3; set manually

	val serverContinuum = new TreeMap[Integer, Server] // Ordered map of locations -> servers on the hash ring

	case class Client(sock: Socket, is: BufferedReader, ps: PrintStream, name: String)
	case class Server(sock: Socket, is: BufferedReader, ps: PrintStream, port: Int)

	def main(args: Array[String]): Unit = {

		implicit val acceptorSystem = ActorSystem("acceptor")
		implicit val clientCommunicatorSystem = ActorSystem("clientCommunicator")
		implicit val serverCommunicatorSystem = ActorSystem("serverCommunicator")

		val clients = new mutable.ArrayBuffer[Client] with mutable.SynchronizedBuffer[Client] {}
		val servers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] {}

		val unhashedServers = new ConcurrentLinkedQueue[Server]()

		// For once I start dealing with fault tolerance
		val disconnectedServers = new mutable.ArrayBuffer[Server] with mutable.SynchronizedBuffer[Server] {}

		val ss = new ServerSocket(controllerPort)

		val acceptor = actor(acceptorSystem)(new Act {
			become {
				case true => {
					while(true) {
						val sock = ss.accept() // IS THIS A POTENTIAL POINT OF FAILURE? SHOULD I BE USING A TRY?
						println("Accepted socket connection.")

						val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))

						val ps = new PrintStream(sock.getOutputStream(), true)

						val adder = actor(acceptorSystem)(new Act {
							become {
								case true => {
									val greeting = is.readLine

									val tokens = greeting.split(" ")

									tokens(0) match {

										case "server" => {
											unhashedServers.add(Server(sock, is, ps, tokens(1).toInt))
											println("Added server at port " + tokens(1) + " to queue of unhashed servers.")

										}

										case _ => {
											clients += Client(sock, is, ps, (clients.length).toString) // NEED TO MAKE CLIENTS UNIQUELY IDENTIFIABLE
											println("Added client to list of clients.")
										}

									}
								}
							}
						})

						adder ! true
					}
				}
			}
		})

		val clientCommunicator = actor(clientCommunicatorSystem)(new Act {
			become {
				case true => {
					while (true) {
						for (client <- clients) {
							if (client.is.ready) {

								val request = client.is.readLine
								println("Received request '" + request + "'' from client.")

								val response = clientSwitchboard(splitRequest(request))

								client.ps.println(response)
								println("Sent response '" + response + "' to client.")
							}
						}
					}
				}
			}
		})


		val serverAdder = actor(serverCommunicatorSystem)(new Act {
			become {
				case true => {
					while (true) {
						Option(unhashedServers.poll()) match {
							case Some(server) => {
								println("Server at port " + server.port + " polled from queue of unhashed servers.")

								if (server.is.ready) { // WILL NEED TO WRITE AN APPROPRIATE MANUAL CONNECT METHOD FOR SERVER

									val request = server.is.readLine
									println("Received request '" + request + "'from server at port " + server.port + ".")

									val response = serverSwitchboard(server, splitRequest(request))

									servers += server
									println("Added server at port " + server.port + " to map of hashed servers.")

									server.ps.println(response)
									println("Sent response '" + response + "' to server at port " + server.port + ".")
								}

							}

							case None => // Do nothing if there is no server in the queue
						}

					}
				}
			}
		})

		acceptor ! true
		serverAdder ! true
		clientCommunicator ! true
		println("Controller online.")
	}

	def addPair(key: String, value: String): String = {

		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		
		serverContinuum.size match {
			
			case 0 => {
				println("No server available; cannot save KVP '" + key + "'.")
				"false"
			}

			case _ => {
				val nearestServerHashValue = Option(serverContinuum.ceilingKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
				val server = serverContinuum(nearestServerHashValue)
				println("Found nearest server at hash value " + nearestServerHashValue + " at port " + server.port + ".")

				server.ps.println("set " + key + " " + value)
				println("Sent command to server at port " + server.port + " to set key '" + key + "' with value '" + value + "'.")
				
				val confirmation = server.is.readLine
				println("Received confirmation from server at port " + server.port + ": '" + confirmation + "'.")

				confirmation
			}
		}
	}

	def addServer(server: Server): String = {
		
		val serverHashValue = hash(server.port.toString)

		serverContinuum(serverHashValue) = server

		if (serverContinuum contains serverHashValue) println("Added server at port " + server.port + " with hash value " + serverHashValue + ".")

		println("Calling migrateKVPs function....")
		migrateKVPs(server)
		println("Called migrateKVPs function.")
		//val migrationConfirmation = server.ps.println("success") /////////////////////////////////////////////////////////////////////

		"success"
	}

	def clientSwitchboard(tokens: Array[String]): String = {
		
		tokens(0) match {
		
			case "delete" => {
				tokens.length match {
					case 1 => "Must enter a key."
					case 2 => {
						deletePair(tokens(1))
					}
					case _ => "Too many arguments."
				}
			}

			case "get" => {
				tokens.length match {
					case 1 => "Must enter a key."
					case 2 => {
						println("Calling getValue function with token '" + tokens(1).toString + "'.")
						getValue(tokens(1))
					}
					case _ => "Too many arguments."
				}
			}

			case "listServers" => {
				tokens.length match {
					case 1 => {
						println("Calling listServers function.")
						listServers()
					}
					case _ => "Too many arguments."
				}
			}

			case "set" => {
				tokens.length match {
					case 1 => "Must enter a key and value."
					case 2 => "Must enter a value."
					case 3 => {
						println("Calling addPair function with tokens '" + tokens(1).toString + "' and '" + tokens(2).toString + "'.")
						addPair(tokens(1), tokens(2))
					}
					case _ => "Too many arguments."
				}
			}

			case "status" => {
				tokens.length match {
					case 1 => {
						println("Calling status function.")
						status()
					}
					case _ => "Too many arguments."
				}
			}

			case _ => "Invalid command."
		}
	}


	def deletePair(key: String): String = {
		
		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		val nearestServerHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		val server = serverContinuum(nearestServerHashValue)
		println("Found nearest server at hash value " + nearestServerHashValue + " at port " + server.port + ".")

		server.ps.println("delete " + key)
		println("Sent command to server at port " + server.port + " to delete KVP with key '" + key + "'.")
		val confirmation = server.is.readLine
		println("Received confirmation '" + confirmation + "' from server at port " + server.port + ".")

		confirmation
	}

	def getValue(key: String): String = {

		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		val serverHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		val server = serverContinuum(serverHashValue)
		println("Found nearest server at hash value " + serverHashValue + " at port " + server.port + ".")

		server.ps.println("get " + key)
		println("Sent command to server at port " + server.port + " to get value associated with key '" + key + "'.")
		val value = server.is.readLine
		println("Received value '" + value + "' from server at port " + server.port + ".")

		value
	}

	def hash(str: String): Int = MurmurHash3.stringHash(str, seed)

	def listServers(): String = {
		println("Generating string of servers on the serverContinuum.")
		serverContinuum.toString
	}

	
	def migrateKVPs(newServer: Server): Unit = {
		
		val newServerHashValue = hash(newServer.port.toString)
		println("Generated hash value " + newServerHashValue + " for server at port " + newServer.port + ".")

		if (serverContinuum.size > 1) {

			// Determine "previous" server on hash ring
			val previousHashValue = Option(serverContinuum.lowerKey(newServerHashValue))

			// Determine "next" server on hash ring
			val nextServerHashValue = Option(serverContinuum.higherKey(newServerHashValue))

			previousHashValue match {

				// Case 1: The new server is the "first" server on the hash ring, clockwise from 12:00
				case None => {
					
					nextServerHashValue match {
						
						case None => // This case will never actually occur, since the serverContinuum must have at least one server for this line to run

						case Some(nextServerHashValue) => {
							val nextServer = serverContinuum(nextServerHashValue)
							// Migrate the keys hashed between the "last" server on the ring and the "end" of the ring from
							// the next server to the new server
							nextServer.ps.println("migrate " + hash(serverContinuum(serverContinuum.lastKey).port.toString) + " " + "end" + " " + seed + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + " to migrate keys between hash values " + 
								hash(serverContinuum(serverContinuum.lastKey).port.toString) + " and the end of the ring with seed " + seed + 
								" to new server at port " + newServer.port + ".")
							val migrationConfirmation1 = nextServer.is.readLine
							println("Received migration confirmation from server at port " + nextServer.port + ": '" + migrationConfirmation1 + "'.")

							
							// Migrate the keys hashed between the "beginning" of the ring and the new server's hash value
							// from the next server to the new server
							nextServer.ps.println("migrate " + "beginning" + " " + newServerHashValue + " " + seed + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + 
								" to migrate keys between the beginning of the hash ring and hash value " + newServerHashValue + " with seed " + seed +
								" to the new server at port " + newServer.port + ".")
							val migrationConfirmation2 = newServer.is.readLine
							println("Received migration confirmation from server at port " + newServer.port + ": '" + migrationConfirmation2 + "'.")
						} 
					}
				}

				case Some(previousServerHashValue) => {
					val previousServer = serverContinuum(previousServerHashValue)
					
					nextServerHashValue match {

						// Case 2: The new server is the "last" server on the hash ring, clockwise from 12:00
						case None => {

							val firstServer = serverContinuum(serverContinuum.firstKey)

							// Migrate the keys hashed between the previous server's hash value and the new server's hash value,
							// from the "first" server on the hash ring to the new server on the hash ring
							firstServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + seed + " " + newServer.port)
							println("Sent command to first server on the hash ring at port " + firstServer.port + 
								" to migrate keys between hash value " + hash(previousServer.port.toString) + " and hash value " + newServerHashValue +
								" with seed " + seed + " to the new server at port " + newServer.port + ".")
							val migrationConfirmation = firstServer.is.readLine
							println("Received migration confirmation from server at port " + firstServer.port + ": '" + migrationConfirmation + "'.")
						}

						// Case 3: The new server is neither the "first" nor the "last" server on the hash ring, clockwise from 12:00
						case Some(nextServerHashValue) => {
							val nextServer = serverContinuum(nextServerHashValue)

							// Migrate the keys hashed between the previous server on the ring and the new server on
							// the ring, from the next server to the new server
							nextServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + seed + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + " to migrate keys between hash value " + 
								hash(previousServer.port.toString) + " and hash value " + newServerHashValue + " with seed " + seed + 
								" to the new server at port " + newServer.port + ".")
							val migrationConfirmation = nextServer.is.readLine
							println("Received migration confirmation from server at port " + nextServer.port + ": '" + migrationConfirmation + "'.")
						}

					}
				}
			}

			
		}

	}


	def serverSwitchboard(server: Server, tokens: Array[String]): String = {

		tokens(0) match {
		
			case "addServer" => {
				addServer(server)
			}

			case _ => {
				"failure"
			}
		}
	}


	def splitRequest(request: String): Array[String] = {

		val splitRequest = request.split(" ")

		splitRequest
	}

	def status(): String = {

		println("status function generating status message.")
		var statusMessage = "\nStatus\n====================\n"

		serverContinuum.size match {

			case 0 => {

				statusMessage += "No servers connected.\n\n"
				
				statusMessage
			}

			case _ => {
				for ((hashValue, server) <- serverContinuum) {
					statusMessage += "--------------------\nPort: " + server.port + "\nHash Value: " + hashValue + "\n"

					server.ps.println("countKVPs")
					val kvpCount = server.is.readLine

					statusMessage += "KVP Count: " + kvpCount + "\n"

					println("Added port " + server.port + ", hash value " + hashValue + ", and KVP count " + kvpCount + " to status message.")
					Option(serverContinuum.lowerKey(hashValue)) match {
						
						case None => {
							val kvpRange = "(-∞, " + hashValue + "]" + " U (" + serverContinuum.lastKey + ", " + "∞)"
							statusMessage += "KVP Range: " + kvpRange + "\n\n"
							println("Added kvpRange for server at port " + server.port + " to status message.")
						}

						case Some(kvpLowValue) => {
							val kvpRange = "(" + kvpLowValue + ", " + hashValue + "]"
							statusMessage += "KVP Range: " + kvpRange + "\n\n"
							println("Added kvpRange for server at port " + server.port + " to status message.")
						}
					}
				}
			}
		}
		
		println("status function returning status message.")
		statusMessage

	}

}
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
											println("Server at port " + tokens(1) + " added to list of unhashed servers.")

										}

										case _ => {
											clients += Client(sock, is, ps, (clients.length).toString) // NEED TO MAKE CLIENTS UNIQUELY IDENTIFIABLE
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
								val response = clientSwitchboard(splitRequest(request))
								client.ps.println(response)
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
								println("Queue size: " + unhashedServers.size + ".")

								if (server.is.ready) { // WILL NEED TO WRITE AN APPROPRIATE MANUAL CONNECT METHOD FOR SERVER
									val request = server.is.readLine
									val response = serverSwitchboard(server, splitRequest(request))

									servers += server

									println("Number of servers in network: " + serverContinuum.size + ".")

									server.ps.println(response)
								}

							}

							case None => // Do nothing if there is no server in the queue
						}

					}
				}
			}
		})
		
		// val serverCommunicator = actor(serverCommunicatorSystem)(new Act {
		// 	become {
		// 		case true => {
		// 			while (true) {
		// 				for (server <- servers) {
		// 					if (server.is.ready) { // THIS IS THE POINT OF FAILURE IF A SERVER IS OFFLINE
		// 						val request = server.is.readLine
		// 						val response = serverSwitchboard(server, splitRequest(request))
		// 						server.ps.println(response)
		// 					}
		// 				}
		// 			}
		// 		}
		// 	}
		// })

		acceptor ! true
		// serverCommunicator ! true
		serverAdder ! true
		clientCommunicator ! true

	}

	def addPair(key: String, value: String): String = {

		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for KVP '" + key + "'.")
		
		serverContinuum.size match {
			
			case 0 => {
				println("No server available; cannot save KVP '" + key + "'.")
				"false"
			}

			case _ => {
				val nearestServerHashValue = Option(serverContinuum.ceilingKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
				val server = serverContinuum(nearestServerHashValue)

				server.ps.println("set " + key + " " + value)
				val confirmation = server.is.readLine

				confirmation
			}
		}
	}

	def addServer(server: Server): String = {

		val serverHashValue = hash(server.port.toString)

		serverContinuum(serverHashValue) = server

		if (serverContinuum contains serverHashValue) println("Added server at port " + server.port + " with hash value " + serverHashValue + ".")

		migrateKVPs(server)

		"success"

	}

	def clientSwitchboard(tokens: Array[String]): String = tokens(0) match {

		case "delete" => {
			tokens.length match {
				case 1 => "Must enter a key."
				case 2 => deletePair(tokens(1))
				case _ => "Too many arguments."
			}
		}

		case "get" => {
			tokens.length match {
				case 1 => "Must enter a key."
				case 2 => getValue(tokens(1))
				case _ => "Too many arguments."
			}
		}

		case "listServers" => {
			tokens.length match {
				case 1 => listServers()
				case _ => "Too many arguments."
			}
		}

		case "set" => {
			tokens.length match {
				case 1 => "Must enter a key and value."
				case 2 => "Must enter a value."
				case 3 => addPair(tokens(1), tokens(2))
				case _ => "Too many arguments."
			}
		}

		case "status" => {
			tokens.length match {
				case 1 => status()
				case _ => "Too many arguments."
			}
		}

		case _ => "Invalid command."
	}


	def deletePair(key: String): String = {

		val kvpHashValue = hash(key)
		val nearestServerHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		val server = serverContinuum(nearestServerHashValue)

		server.ps.println("delete " + key)
		val confirmation = server.is.readLine

		confirmation
	}

	def getValue(key: String): String = {
		val kvpHashValue = hash(key)
		val serverHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		val server = serverContinuum(serverHashValue)

		server.ps.println("get " + key)
		val value = server.is.readLine

		value
	}

	def hash(str: String): Int = {
		MurmurHash3.stringHash(str, seed)
	}

	def listServers(): String = serverContinuum.toString

	
	def migrateKVPs(newServer: Server): Unit = {

		val newServerHashValue = hash(newServer.port.toString)

		if (serverContinuum.size > 1) {

			// Determine "previous" server on hash ring
			val previousServer = Option(serverContinuum(serverContinuum.lowerKey(newServerHashValue)))

			// Determine "next" server on hash ring
			val nextServer = Option(serverContinuum(serverContinuum.higherKey(newServerHashValue)))

			previousServer match {

				// Case 1: The new server is the "first" server on the hash ring, clockwise from 12:00
				case None => {
					
					nextServer match {
						
						case None => {} // THIS WILL NEVER HAPPEN, SO I'M NOT SURE WHAT TO DO HERE

						case Some(nextServer) => {
							// Migrate the keys hashed between the "last" server on the ring and the "end" of the ring from
							// the next server to the new server
							nextServer.ps.println("migrate " + hash(serverContinuum(serverContinuum.lastKey).port.toString) + " " + "end" + " " + seed + " " + newServer.port)

							// Migrate the keys hashed between the "beginning" of the ring and the new server's hash value
							// from the next server to the new server
							nextServer.ps.println("migrate " + "beginning" + " " + newServerHashValue + " " + seed + " " + newServer.port)
						} 
					}
				}

				case Some(previousServer) => {

					nextServer match {
						
						// Case 2: The new server is the "last" server on the hash ring, clockwise from 12:00
						case None => {
							val firstServer = serverContinuum(serverContinuum.firstKey)

							// Migrate the keys hashed between the previous server's hash value and the new server's hash value,
							// from the "first" server on the hash ring to the new server on the hash ring
							firstServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + newServer.port)
						}

						// Case 3: The new server is neither the "first" nor the "last" server on the hash ring, clockwise from 12:00
						case Some(nextServer) => {
							// Migrate the keys hashed between the previous server on the ring and the new server on
							// the ring, from the next server to the new server
							nextServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + newServer.port)
						}

					}
				}
			}

			
		}

	}


	def serverSwitchboard(server: Server, tokens: Array[String]): String = tokens(0) match {

		case "addServer" => addServer(server)

		case _ => "failure"
	}


	def splitRequest(request: String): Array[String] = {
		request.split(" ")
	}

	def status(): String = {
		var statusMessage = "\nStatus\n====================\n"

		serverContinuum.size match {

			case 0 => {
				statusMessage += "No servers connected.\n\n"
				statusMessage
			}

			case _ => {
				for ((location, server) <- serverContinuum) {
					statusMessage += "--------------------\nPort: " + server.port + "\nHash Location: " + location + "\n"

					server.ps.println("countKVPs")
					val kvpCount = server.is.readLine

					statusMessage += "KVP Count: " + kvpCount + "\n"

					Option(serverContinuum.lowerKey(location)) match {
						
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
			}
		}
		
		statusMessage

	}

}
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
						println("Establishing InputStream.")

						val ps = new PrintStream(sock.getOutputStream(), true)
						println("Establishing PrintStream.")

						val adder = actor(acceptorSystem)(new Act {
							become {
								case true => {
									val greeting = is.readLine
									println("Received greeting '" + greeting + "'.")

									val tokens = greeting.split(" ")
									println("Split greeting to '" + tokens.toString + "'")

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
								println("Found client InputStream ready.")

								val request = client.is.readLine
								println("Received request '" + request + "'' from client.")

								val response = clientSwitchboard(splitRequest(request))
								println("Generated response '" + response + "'' for client.")

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
								println("Found a server in the queue of unhashed servers.")
								println("Server at port " + server.port + " polled from queue of unhashed servers.")
								println("Current queue size: " + unhashedServers.size + ".")

								if (server.is.ready) { // WILL NEED TO WRITE AN APPROPRIATE MANUAL CONNECT METHOD FOR SERVER
									println("Determined InputStream for server at port " + server.port + " is ready.")

									val request = server.is.readLine
									println("Received request '" + request + "'from server at port " + server.port + ".")

									val response = serverSwitchboard(server, splitRequest(request))
									println("Generated response '" + response + "' for server at port " + server.port + ".")

									servers += server
									println("Added server at port " + server.port + " to map of hashed servers.")
									println("Current number of servers in network: " + serverContinuum.size + ".")

									server.ps.println(response)
									println("Sending response '" + response + "' to server at port " + server.port + ".")
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
		println("Activated akka actor to accept socket connections.")
		// serverCommunicator ! true
		serverAdder ! true
		println("Activated akka actor to add new servers to the network.")
		clientCommunicator ! true
		println("Activated akka actor to communicate with clients.")

	}

	def addPair(key: String, value: String): String = {
		println("addPair function received key '" + key + "' and value '" + value + "'.")

		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		
		serverContinuum.size match {
			
			case 0 => {
				println("No server available; cannot save KVP '" + key + "'.")
				"false"
			}

			case _ => {
				println("Found at least one server on the network.")
				val nearestServerHashValue = Option(serverContinuum.ceilingKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
				println("Found nearest server on hash ring at hash value " + nearestServerHashValue + ".")
				val server = serverContinuum(nearestServerHashValue)
				println("Found nearest server at hash value " + nearestServerHashValue + "at port " + server.port + ".")

				server.ps.println("set " + key + " " + value)
				println("Sent command to server at port " + server.port + " to set key '" + key + "' with value '" + value + "'.")
				
				val confirmation = server.is.readLine
				println("Received confirmation from server at port " + server.port + ": '" + confirmation + "'.")

				println("addPair function returning confirmation '" + confirmation + "' from addPair function.")
				confirmation
			}
		}
	}

	def addServer(server: Server): String = {
		println("addServer function received server at port " + server.port + ".")
		
		val serverHashValue = hash(server.port.toString)
		println("Generated hash value " + serverHashValue + " for server at port " + server.port + ".")

		serverContinuum(serverHashValue) = server

		if (serverContinuum contains serverHashValue) println("Added server at port " + server.port + " with hash value " + serverHashValue + ".")

		println("Calling migrateKVPs function....")
		migrateKVPs(server)
		println("Called migrateKVPs function.")

		println("addServer function returning message 'success'.")
		"success"

	}

	def clientSwitchboard(tokens: Array[String]): String = {
		println("clientSwitchboard function received tokens '" + tokens.toString + "'.")
		
		println("Matching command token(0).")
		tokens(0) match {
		
			case "delete" => {
				println("Determined command token(0) to be 'delete'.")
				tokens.length match {
					case 1 => "Must enter a key."
					case 2 => {
						println("Calling deletePair function with token '" + tokens(1).toString + "'.")
						deletePair(tokens(1))
					}
					case _ => "Too many arguments."
				}
			}

			case "get" => {
				println("Determined command token(0) to be 'get'.")
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
				println("Determined command token(0) to be 'listServers'.")
				tokens.length match {
					case 1 => {
						println("Calling listServers function.")
						listServers()
					}
					case _ => "Too many arguments."
				}
			}

			case "set" => {
				println("Determined command token(0) to be 'set'.")
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
				println("Determined command token(0) to be 'status'.")
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
		println("deletePair function received key '" + key + "'.")
		
		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		val nearestServerHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		println("Found nearest server on hash ring at hash value " + nearestServerHashValue + ".")
		val server = serverContinuum(nearestServerHashValue)
		println("Found nearest server at hash value " + nearestServerHashValue + " at port " + server.port + ".")

		server.ps.println("delete " + key)
		println("Sent command to server at port " + server.port + " to delete KVP with key '" + key + "'.")
		val confirmation = server.is.readLine
		println("Received confirmation '" + confirmation + "' from server at port " + server.port + ".")

		println("deletePair function returning message '" + confirmation + "'.")
		confirmation
	}

	def getValue(key: String): String = {
		println("getValue function received key '" + key + "'.")

		val kvpHashValue = hash(key)
		println("Generated hash value " + kvpHashValue + " for key '" + key + "'.")
		val serverHashValue = Option(serverContinuum.higherKey(kvpHashValue)).getOrElse(serverContinuum.firstKey)
		println("Found nearest server on hash ring at hash value " + serverHashValue + ".")
		val server = serverContinuum(serverHashValue)
		println("Found nearest server at hash value " + serverHashValue + " at port " + server.port + ".")

		server.ps.println("get " + key)
		println("Sent command to server at port " + server.port + " to get value associated with key '" + key + "'.")
		val value = server.is.readLine
		println("Received value '" + value + "' from server at port " + server.port + ".")

		println("getValue function returning value '" + value + "'.")
		value
	}

	def hash(str: String): Int = {
		println("hash function received string '" + str + "'.")

		val hashValue = MurmurHash3.stringHash(str, seed)
		
		println("hash function returning hash value " + hashValue + ".")
		hashValue
	}

	def listServers(): String = {
		println("listServers function called.")
		println("listServers function returning serverContinuum converted to string.")
		serverContinuum.toString
	}

	
	def migrateKVPs(newServer: Server): Unit = {
		println("migrateKVPs function received server at port " + newServer.port + ".")
		
		val newServerHashValue = hash(newServer.port.toString)
		println("Generated hash value " + newServerHashValue + " for server at port " + newServer.port + ".")

		if (serverContinuum.size > 1) {
			println("Determined serverContinuum contains more than one server.")
			
			// Determine "previous" server on hash ring
			val previousServer = Option(serverContinuum(serverContinuum.lowerKey(newServerHashValue)))
			println("Found previous server on hash ring.")

			// Determine "next" server on hash ring
			val nextServer = Option(serverContinuum(serverContinuum.higherKey(newServerHashValue)))
			println("Found next server on hash ring.")

			println("Matching previous server on the hash ring.")
			previousServer match {

				// Case 1: The new server is the "first" server on the hash ring, clockwise from 12:00
				case None => {
					println("Found that there is no previous server on the hash ring.")
					
					println("Matching next server on the hash ring.")
					nextServer match {
						
						case None => println("Determined that there exists no next server on the hash ring.") // THIS WILL NEVER HAPPEN, SO I'M NOT SURE WHAT TO DO HERE

						case Some(nextServer) => {
							println("Determined that there exists some next server on the hash ring.")
							// Migrate the keys hashed between the "last" server on the ring and the "end" of the ring from
							// the next server to the new server
							nextServer.ps.println("migrate " + hash(serverContinuum(serverContinuum.lastKey).port.toString) + " " + "end" + " " + seed + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + " to migrate keys between hash values " + 
								hash(serverContinuum(serverContinuum.lastKey).port.toString) + " and the end of the ring with seed " + seed + 
								" to new server at port " + newServer.port + ".")
							
							// Migrate the keys hashed between the "beginning" of the ring and the new server's hash value
							// from the next server to the new server
							nextServer.ps.println("migrate " + "beginning" + " " + newServerHashValue + " " + seed + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + 
								" to migrate keys between the beginning of the hash ring and hash value " + newServerHashValue + " with seed " + seed +
								" to the new server at port " + newServer.port + ".")
						} 
					}
				}

				case Some(previousServer) => {
					println("Determined that there exists some previous server on the hash ring.")
					
					println("Matching next server on the hash ring.")
					nextServer match {

						// Case 2: The new server is the "last" server on the hash ring, clockwise from 12:00
						case None => {
							println("Determined that there exists no next server on the hash ring.")

							val firstServer = serverContinuum(serverContinuum.firstKey)
							println("Found first server on the hash ring at port " + firstServer.port + ".")

							// Migrate the keys hashed between the previous server's hash value and the new server's hash value,
							// from the "first" server on the hash ring to the new server on the hash ring
							firstServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + seed + " " + newServer.port)
							println("Sent command to first server on the hash ring at port " + firstServer.port + 
								" to migrate keys between hash value " + hash(previousServer.port.toString) + " and hash value " + newServerHashValue +
								" with seed " + seed + " to the new server at port " + newServer.port + ".")
						}

						// Case 3: The new server is neither the "first" nor the "last" server on the hash ring, clockwise from 12:00
						case Some(nextServer) => {
							println("Determined that there exists some next server on the hash ring at port " + nextServer.port + ".")

							// Migrate the keys hashed between the previous server on the ring and the new server on
							// the ring, from the next server to the new server
							nextServer.ps.println("migrate " + hash(previousServer.port.toString) + " " + newServerHashValue + " " + newServer.port)
							println("Sent command to next server at port " + nextServer.port + " to migrate keys between hash value " + 
								hash(previousServer.port.toString) + " and hash value " + newServerHashValue + " with seed " + seed + 
								" to the new server at port " + newServer.port + ".")
						}

					}
				}
			}

			
		}

	}


	def serverSwitchboard(server: Server, tokens: Array[String]): String = {
		println("serverSwitchboard function received server at port " + server.port + " and tokens '" + tokens.toString + "'.")

		tokens(0) match {
		
			case "addServer" => {
				println("Determined command token(0) to be 'addServer'.")
				println("Calling addServer function to server at port " + server.port + ".")
				addServer(server)
			}

			case _ => {
				println("Determined command token(0) to be unrecognizable.")
				println("Returning message 'failure'.")
				"failure"
			}
		}
	}


	def splitRequest(request: String): Array[String] = {
		println("splitRequest function received request '" + request + "'.")

		val splitRequest = request.split(" ")

		println("splitRequest function returning '" + splitRequest.toString + ".")
		splitRequest
	}

	def status(): String = {
		println("status function called.")

		var statusMessage = "\nStatus\n====================\n"

		println("Matching serverContinuum size.")
		serverContinuum.size match {

			case 0 => {
				println("Determined that there are no servers in the serverContinuum.")

				statusMessage += "No servers connected.\n\n"
				
				println("status function returning status message.")
				statusMessage
			}

			case _ => {
				println("Determined that there is at least one server in the serverContinuum.")
				println("Iterating over the servers in the serverContinuum.")
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
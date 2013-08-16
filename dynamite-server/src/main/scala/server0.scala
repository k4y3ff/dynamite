import akka.actor.ActorSystem
import akka.actor.ActorDSL._

import collection.mutable

import java.io.{ BufferedReader, InputStreamReader, PrintStream	}
import java.net.{ InetSocketAddress, ServerSocket, Socket }
import java.util.concurrent.ConcurrentLinkedQueue

import scala.util.hashing.MurmurHash3
import scala.util.{ Try, Success, Failure }

object server0 extends App {

	implicit val peerServerAcceptorSystem = ActorSystem("peerserveracceptorsystem")
	implicit val peerServerCommunicatorSystem = ActorSystem("peerservercommunicatorsystem")
	implicit val controllerCommunicatorSystem = ActorSystem("controllercommunicatorsystem")
	implicit val terminalCommunicatorSystem = ActorSystem("terminalcommunicatorsystem")

	val host = "localhost"
	val controllerPort = 4343 // Port of the head node
	val serverPort = 4000 // Port of this server

	var connectedFlag = false // Flag that shows whether or not this server has been initially added to the network

	case class Controller(sock: Socket, is: BufferedReader, ps: PrintStream, name: String) // NEED TO MAKE CONTROLLERS UNIQUELY IDENTIFIABLE?
	case class PeerServer(sock: Socket, is: BufferedReader, ps: PrintStream)
	
	val controllers = new mutable.ArrayBuffer[Controller] with mutable.SynchronizedBuffer[Controller] {} // Need a place to store the controller connection...?

	val peerServers = new ConcurrentLinkedQueue[PeerServer]()
	
	val kvStore = collection.mutable.Map[String, String]()


	val ss = new ServerSocket(serverPort)

	val controllerInputReader = actor(controllerCommunicatorSystem)(new Act {

		become {
			case true => while (true) { // Things to do whenever the controller connects and sends something to the server
				for (controller <- controllers) {
					if (controller.is.ready) {
						val request = controller.is.readLine
						println("Request: " + request)

						controller.ps.println(controllerSwitchboard(splitRequest(request)))
					}
				}
			}
		}

	})

	val peerServerInputReader = actor(peerServerCommunicatorSystem)(new Act {

		become {
			case true => while (true) {
				Option(peerServers.poll()) match {
					case Some(peerServer) => {
						println("Found a peer server in the queue of peer servers.")
						println("Server polled from queue of peer servers.")
						println("Current peer server queue size: " + peerServers.size + ".")

						var peerServerFlag = true

						while (peerServerFlag) {
							println("Determined InputStream for peer server is ready.")
							
							val request = peerServer.is.readLine
							println("Received request '" + request + "' from peer server.")

							request match {
								case "EOT" => {
									peerServerFlag = false
									println("Set peerServerFlag to false.")
								}
								case _ => {
									val response = controllerSwitchboard(splitRequest(request))
									println("Generated response '" + response + "' for peer server.")

									println("Sending response '" + response + "' to peer server.")
									peerServer.ps.println(response)
								}	
							}
						}

						println("Sending message 'EOT' to peer server.")
						peerServer.ps.println("EOT")
					}

					case None => // Do nothing if there is no peer server in the queue
				}
			}
		}

	})

	val peerServerAcceptor = actor(peerServerAcceptorSystem)(new Act {

		become {
			case true => {
				println("Server accepting TCP connections from peers.")

				while (true) {
					val sock = ss.accept()
					val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
					val ps = new PrintStream(sock.getOutputStream())

					// This is what happens whenever a new peer server connects with the server.
					val peerServerAdder = actor(peerServerAcceptorSystem)(new Act {

						become {
							case true => peerServers.add(PeerServer(sock, is, ps)) // Should replace this name with something more sensible
						}

					})

					peerServerAdder ! true
				}
			}
		}

	})


	val terminalInputReader = actor(terminalCommunicatorSystem)(new Act{
		become {
			case true => while (true) {
				val terminalInput = readLine

				terminalInput match {

					case "connect" => {
						// controllerAcceptor ! true // IS THIS PROBLEMATIC, BECAUSE THE ACTOR MAY ALREADY BE RUNNING?
						controllerInputReader ! true // IS THIS PROBLEMATIC, BECAUSE THE ACTOR MAY ALREADY BE RUNNING?
						connectToDatabase()
					}

					case "disconnect" => {
						controllerCommunicatorSystem.shutdown
						peerServerAcceptorSystem.shutdown
						
						ss.close()

						println("Server offline.\n")
					}

				}
			}
		}
	})


	controllerInputReader ! true
	peerServerAcceptor ! true
	peerServerInputReader ! true
	terminalInputReader ! true

	connectToDatabase()


	def connectToDatabase(): Boolean = {

		connectedFlag match {

			case false => {
				val sock = new Socket() // Potential point of failure
				// sock.setKeepAlive(true)

				Try(sock.connect(new InetSocketAddress(host, controllerPort), 5000)) match {
					case Success(_) => {
						println("Successfully connected to controller at port " + controllerPort + ".")
						
						def addSelfToRing(): Boolean = {

							val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
							val ps = new PrintStream(sock.getOutputStream())

							controllers += Controller(sock, is, ps, controllers.length.toString)

							ps.println("server " + serverPort) // Potential point of failure

							ps.println("addServer " + serverPort) // Potential point of failure

							val confirmation = is.readLine // Potential point of failure

							confirmation match {
								case "success" => {
									println("Successfully added server to database.")
									true
								}

								case "failure" => {
									println("Failed to add server to database.")
									false
								}
							}
						}

						addSelfToRing()

					}
					case Failure(_) => {
						println("Unable to connect to controller at port " + controllerPort + ".")
						false
					}
				}
			}

			case true => true
		}

	}

	
	def controllerSwitchboard(tokens: Array[String]): String = tokens(0) match {

		case "countKVPs" 	=> kvStore.size.toString
		case "delete" 		=> delete(tokens.slice(1, 3))
		case "get"			=> get(tokens.slice(1, 2))
		case "migrate"		=> migrate(tokens.slice(1, 5)); "success1"
		case "set"			=> set(tokens.slice(1, 3))
		case _				=> "failure"

	}


	// Removes a single KVP from kvStore
	def delete(tokens: Array[String]): String = {
		
		kvStore.remove(tokens(0)) match {
			
			case Some(_) => {
				println("Key '" + tokens(0) + "' deleted from server.")
				"success"
			}
			
			case None => {
				println("Failed to delete key '" + tokens(0) + "'' from server; no value found.")
				"failure"
			}

		}
	}

	// Returns a value, given a key
	def get(tokens: Array[String]): String = {

		val value = kvStore getOrElse(tokens(0), "failure") // This is problematic, because someone might want to store the string "failure"
		println("Value associated with key '" + tokens(0) + "' retrieved from server.")

		value

	}

	def migrate(tokens: Array[String]): Unit = { // THIS FUNCTION SHOULD RETURN A CONFIRMATION

		println("migrate function received array of length " + tokens.length + ".")

		// Establish low end of hash range
		val lowHashValueStr = tokens(0)
		println("Received lowHashValue '" + tokens(0) + "'.")
		// Establish high end of hash range
		val highHashValueStr = tokens(1)
		println("Received highHashValue '" + tokens(1) + "'.")
		// Establish seed for hash function
		val seed = tokens(2).toInt
		println("Received seed value " + seed + ".")
		// Establish port of new server
		val newServerPort = tokens(3).toInt
		println("Received port number " + tokens(3) + ".")

		// Open connection to the new server
		val sock = new Socket()

		Try(sock.connect(new InetSocketAddress(host, newServerPort), 5000)) match {
			case Success(_) => migrateKVPs()
			case Failure(_) => println("Failed to migrate KVPs hashed between " + lowHashValueStr + " and " + highHashValueStr + " to server at port " + newServerPort + ".")
		}

		def migrateKVPs() {

			val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
			val ps = new PrintStream(sock.getOutputStream())

			highHashValueStr match {

				case "end" => {
					val lowHashValue = lowHashValueStr.toInt

					for ((key, value) <- kvStore) {
						val keyHashValue = hash(key)

						if (keyHashValue > lowHashValue) {
							ps.println("set " + key + " " + value)

							val confirmation = is.readLine

							confirmation match {
								case "success" => {
									println(delete(Array(key)))
								}

								case _ => // NEED A PROPER FAILURE CASE HERE
							}
						}
					}
				}

				case _ => {

					lowHashValueStr match {

						case "beginning" => {
							val highHashValue = highHashValueStr.toInt

							for ((key, value) <- kvStore) {
								val keyHashValue = hash(key)

								if (keyHashValue <= highHashValue) {
									ps.println("set " + key + " " + value)

									val confirmation = is.readLine

									confirmation match {
										
										case "success" => {
											println(delete(Array(key)))
										}

										case _ => // NEED A PROPER FAILURE CASE HERE
									}
								}
							}
						}

						case _ => {

							val lowHashValue = lowHashValueStr.toInt
							val highHashValue = highHashValueStr.toInt

							for ((key, value) <- kvStore) {
								println("Iterating over key '" + key + "' and value '" + value + "'.")
								val keyHashValue = hash(key)
								println("Generated hash value " + keyHashValue + " for key " + key + ".")

								if (keyHashValue <= highHashValue && keyHashValue > lowHashValue) {
									println("Determined that '" + key + "' is within the given hash range.")
									ps.println("set " + key + " " + value)
									println("Sent command to server at port " + newServerPort + " to set key '" + key + "' with value '" + value + "'.")
									val confirmation = is.readLine
									println("Received confirmation '" + confirmation + "' from server at port " + newServerPort + ".")
									
									println("Matching confirmation.")
									confirmation match {

										case "success" => {
											println("Determined confirmation to be 'success'.")

											println(delete(Array(key)))
										}

									}
								}
							}

							println("Sending message 'EOT' to server at port " + newServerPort + ".")
							ps.println("EOT")
							println("Sent message 'EOT' to server at port " + newServerPort + ".")

							println("Waiting to receive confirmation message from server at port " + newServerPort + ".")
							val closeConfirmation = is.readLine()
							println("Received confirmation message '" + closeConfirmation + "' from server at port " + newServerPort + ".")

						}

					}

				}

			}

			sock.close()
			println("Closed socket to server at port " + newServerPort + ".")

			def hash(str: String): Int = {
				MurmurHash3.stringHash(str, seed)
			}

		}

	}

	// Adds or updates a key-value pair to kvStore
	def set(tokens: Array[String]): String = {
		kvStore(tokens(0)) = tokens(1)

		(kvStore(tokens(0)) == tokens(1)) match {
			
			case true => {
				println("Successfully updated key '" + tokens(0) + "'.")
				"success"
			}

			case _ => {
				println("Failed to update key '" + tokens(0) + "'.")
				"failure"
			}
		}
	}

	def splitRequest(request: String): Array[String] = {
		request.split(" ")
	}

}
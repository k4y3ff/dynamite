import akka.actor.ActorSystem
import akka.actor.ActorDSL._

import collection.mutable

import java.net.{ InetSocketAddress, ServerSocket, Socket }
import java.io.{ BufferedReader, InputStreamReader, PrintStream	}

import scala.util.hashing.MurmurHash3
import scala.util.{ Try, Success, Failure }

object server0 extends App {

	implicit val controllerAcceptorSystem = ActorSystem("controlleracceptorsystem")
	implicit val controllerCommunicatorSystem = ActorSystem("controllercommunicatorsystem")
	implicit val terminalCommunicatorSystem = ActorSystem("terminalcommunicatorsystem")

	val host = "localhost"
	val controllerPort = 4343 // Port of the head node
	val serverPort = 4000 // Port of this server

	var connectedFlag = false // Flag that shows whether or not this server has been initially added to the network

	case class Controller(sock: Socket, is: BufferedReader, ps: PrintStream, name: String) // NEED TO MAKE CONTROLLERS UNIQUELY IDENTIFIABLE?

	val controllers = new mutable.ArrayBuffer[Controller] with mutable.SynchronizedBuffer[Controller] {} // Need a place to store the controller connection...?

	val kvStore = collection.mutable.Map[String, String]()


	val ss = new ServerSocket(serverPort)

	// val controllerAcceptor = actor(controllerAcceptorSystem)(new Act {

	// 	become {
	// 		case true => {
	// 			println("Server online.") // Technically, this should be after I start accepting connections, not before....

	// 			while (true) {
	// 				val sock = ss.accept()
	// 				val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
	// 				val ps = new PrintStream(sock.getOutputStream())

	// 				// This is what happens whenever a new controller connects with the server.
	// 				val controllerAdder = actor(controllerAcceptorSystem)(new Act {

	// 					become {
	// 						case true => controllers += Controller(sock, is, ps, (controllers.length + 1).toString) // Should replace this name with something more sensible
	// 					}

	// 				})

	// 				controllerAdder ! true
	// 			}
	// 		}
	// 	}

	// })

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
						controllerAcceptorSystem.shutdown
						
						ss.close()

						println("Server offline.\n")
					}

				}
			}
		}
	})

	// controllerAcceptor ! true
	controllerInputReader ! true
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
		case "delete" 		=> delete(tokens.slice(1, 3)); "success"
		case "get"			=> get(tokens.slice(1, 2))
		case "migrate"		=> migrate(tokens.slice(1, 5)); "success"
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

		// Establish low end of hash range
		val lowHashValueStr = tokens(0)
		// Establish high end of hash range
		val highHashValueStr = tokens(1)
		// Establish seed for hash function
		val seed = tokens(2).toInt
		// Establish port of new server
		val newServerPort = tokens(3).toInt

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
									val args = new Array[String](1)
									args(0) = key
									delete(args)
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
											val args = new Array[String](1)
											args(0) = key
											delete(args)
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
								val keyHashValue = hash(key)

								if (keyHashValue <= highHashValue && keyHashValue > lowHashValue) {
									ps.println("set " + key + " " + value)

									val confirmation = is.readLine

									confirmation match {

										case "success" => {
											val args = new Array[String](1)
											args(0) = key
											delete(args)
										}

									}
								}
							}

						}

					}

				}

			}

			sock.close()

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
















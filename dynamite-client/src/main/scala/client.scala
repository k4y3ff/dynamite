package client

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import java.net.Socket
import java.net.InetSocketAddress
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

import scala.util.control._
import scala.util.{ Try, Success, Failure }

object client extends App {

	val host = "localhost"
	val controllerPort = 4343

	val sock = new Socket()

	var connectedFlag = false // Reflects whether or not the client is connected to the database

	implicit val controllerSystem = ActorSystem("controllersystem")
	implicit val terminalSystem = ActorSystem("terminalsystem")

	connectToDatabase()

	def batchSet(is: BufferedReader, ps: PrintStream, terminalInput: String): String = {
		var overallSuccess = "success"

		val filename = terminalInput.split(" ")(1)

		val currentDirectory = new java.io.File(".").getCanonicalPath
		val filepath = currentDirectory + "/data/" + filename

		val loop = new Breaks

		loop.breakable {
			for (line <- scala.io.Source.fromFile(filepath).getLines) {
				ps.println("set " + line)
				val confirmation = is.readLine()

				if (confirmation != "success") {
					overallSuccess = confirmation
					loop.break
				}
			}
		}

		"success"
	}

	def connectToDatabase(): Boolean = {

		connectedFlag match {
			case false => {

				Try(sock.connect(new InetSocketAddress(host, controllerPort), 15000)) match {
					case Success(_) => {
						sendAndReceive()
					}

					case Failure(_) => {
						println("Failed to connect to database.\n")
						false
					}
				}
			}

			case true => false // If the client is already connected to the database, do nothing
		}
	}

	def firstToken(input: String): String = {
		input.slice(0, input.indexOf(" "))
	}	

	def sendAndReceive(): Boolean = {
		val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
		val ps = new PrintStream(sock.getOutputStream())

		ps.println("client") // Sends message to controller to establish self as client

		val controllerCommunicator = actor(controllerSystem)(new Act {
			become {
				case true => while (true) {
					if (is.ready) {
						val output = is.readLine
						println(output + "\n\n")
					}

					Thread.sleep(100)
				}
			}
		})
		
		val terminalCommunicator = actor(terminalSystem)(new Act {
			become {
				case true => {
					var flag = true

					while (flag) {
						val input = readLine

						firstToken(input) match {
							case "import"	=> {
								println(batchSet(is, ps, input) + "\n\n")
							}
							case "quit" 	=> flag = false
							case _ 			=> ps.println(input)
						}
					}

					sock.close()
				}
			}
		})

		controllerCommunicator ! true
		terminalCommunicator ! true

		true
	}

}
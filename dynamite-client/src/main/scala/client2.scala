package client

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import java.net.Socket
import java.net.InetSocketAddress
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

import scala.util.{ Try, Success, Failure }

object client2 extends App {

	val host = "localhost"
	val controllerPort = 4343

	val sock = new Socket()

	var connectedFlag = false // Reflects whether or not the client is connected to the database

	implicit val controllerSystem = ActorSystem("controllersystem")
	implicit val terminalSystem = ActorSystem("terminalsystem")

	connectToDatabase()

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

						input match {
							case "quit" => flag = false
							case _ 		=> ps.println(input)
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
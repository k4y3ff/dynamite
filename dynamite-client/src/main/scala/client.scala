package client

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import java.net.Socket
import java.net.InetSocketAddress
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

import scala.util.{ Try, Success, Failure }

object client {
	
	val host = "localhost"
	val port = 4343

	def main(args: Array[String]): Unit = {
		
		implicit val system = ActorSystem()

		val sock = new Socket()
		println("Created new socket.")
		
		sock.setKeepAlive(true)
		println("Set socket keep-alive to true.")
		
		println("Socket timeout value: " + sock.getSoTimeout + ".")
		println("Socket keep-alive: " + sock.getKeepAlive + ".")
		println("Socket linger: " + sock.getSoLinger + ".")
		
		println("Trying to connect socket to server socket on host '" + host + "' at port " + port + " with timeout value of 15000.")
		Try(sock.connect(new InetSocketAddress(host, port), 15000)) match {
			case Success(_) => {
				println("Successfully connected to server socket on host '" + host + "' at port " + port + ".")
				println("Calling sendAndReceive function.")
				sendAndReceive()
			}
			case Failure(_) => println("Database offline.\n")
		}

		def sendAndReceive(): Unit = {
			println("sendAndReceive function has been called.")

			val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
			println("Created new InputStream.")
			val ps = new PrintStream(sock.getOutputStream())
			println("Created new PrintStream.")

			println("Socket timeout value: " + sock.getSoTimeout + ".")
			println("Socket keep-alive: " + sock.getKeepAlive + ".")
			println("Socket linger: " + sock.getSoLinger + ".")

			ps.println("client")
			println("Sent message to controller to establish self as client.")

			var flag = true
			println("Set flag equal to " + flag.toString + ".")

			val outputer = actor(new Act {

				become {
					case true => while (true) {
						if (is.ready) { // Potential point of failure
							val output = is.readLine
							print(output + "\n\n")
						}

						Thread.sleep(100)
					}
				}

			})

			outputer ! true
			println("Akka actor 'outputer' activated.")

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

}
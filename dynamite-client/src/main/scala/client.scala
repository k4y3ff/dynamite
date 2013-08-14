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

		sock.setKeepAlive(true)
		
		println(sock.getSoTimeout)
		println(sock.getKeepAlive)
		println(sock.getSoLinger)

		Try(sock.connect(new InetSocketAddress(host, port), 15000)) match {
			case Success(_) => sendAndReceive()
			case Failure(_) => println("Database offline.\n")
		}

		def sendAndReceive(): Unit = {
			val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
			val ps = new PrintStream(sock.getOutputStream())

			println(sock.getSoTimeout)
			println(sock.getKeepAlive)
			println(sock.getSoLinger)

			ps.println("client")

			var flag = true

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
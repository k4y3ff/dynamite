package main

import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import java.net.Socket
import java.io.BufferedReader
import java.io.PrintStream
import java.io.InputStreamReader

object client {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("outputer")

    val host = "localhost"
    val port = 4343
    val sock = new Socket(host, port)
    val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
    val ps = new PrintStream(sock.getOutputStream())
    var flag = true

    val outputer = actor(new Act {
      become {
        case true => while(true) { 
          if(is.ready) { 
            val output = is.readLine 
            println(output) 
          } 
          Thread.sleep(100) 
        }
      }
    })

    outputer ! true

    // actors.Actor.actor {
    //   while(flag) {
    //     if(is.ready) {
    //       val output = is.readLine
    //       println(output)
    //     }
    //     Thread.sleep(100)
    //   }
    // }

    
    while(flag) {
      val input = readLine
      if(input == "quit") flag = false
      else ps.println(input)
    }
    
    sock.close()
  }
 
}
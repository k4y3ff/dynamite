import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import java.net.ServerSocket
import java.io.PrintStream
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import collection.mutable

object server1 {
  
  implicit val system = ActorSystem("coodinatoracceptor")

  val kvStore = collection.mutable.Map[String, String]()
  
  def main(args: Array[String]): Unit = {
    case class Coordinator(sock:Socket, is:BufferedReader, ps:PrintStream, name:String)
    
    val coordinators = new mutable.ArrayBuffer[Coordinator] with mutable.SynchronizedBuffer[Coordinator] {}
    val ss = new ServerSocket(4010)
    
    val coordinatorAcceptor = actor(system)(new Act {
      become {
        case true => {
          while(true) {
            val sock = ss.accept()
            val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
            val ps = new PrintStream(sock.getOutputStream())

            // This is what happens whenever a new coordinator connects with the server.
            val coordinatorAdder = actor(system)(new Act {
              become {
                case true => coordinators += Coordinator(sock, is, ps, (coordinators.length + 1).toString)
              }
            })

            coordinatorAdder ! true
          }
        }
      }
    })

    coordinatorAcceptor ! true
    
    
    // Things to do whenever a client sends something to the server.
    while(true) {
      for(coordinator <- coordinators) {
        if(coordinator.is.ready) {
          val request = coordinator.is.readLine 
          coordinator.ps.println(switchboard(request))
        }
      }
    }
  }
  
  // Removes a single KVP from kvStore
  def delete(tokens:Array[String]): Unit = kvStore.remove(tokens(0))

  // Returns a value, given a key
  def get(tokens:Array[String]): String = kvStore getOrElse (tokens(0), "No key with that name.")
  
  // Adds a new key-value pair to kvStore
  def set(tokens:Array[String]): String = {
    kvStore(tokens(0)) = tokens(1)
    "Key '" + tokens(0) + "' assigned value '" + tokens(1) + "'."
  }
  
  def switchboard(request:String): String = {
    val tokens = request.split(" ")
    val command = tokens(0)
    command match {
      case "delete" => delete(tokens.slice(1,3)); return "true"
      case "get"		=> return get(tokens.slice(1,2))
      case "set"		=> return set(tokens.slice(1,3))
      case other		=> return "Command not found."
    }
    
  }
    
}
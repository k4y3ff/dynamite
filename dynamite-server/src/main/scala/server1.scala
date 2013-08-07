import akka.actor.ActorDSL._
import akka.actor.ActorSystem

import collection.mutable

import java.net.ServerSocket
import java.io.PrintStream
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

import scala.util.hashing.MurmurHash3

object server1 {

  implicit val system = ActorSystem("coodinatoracceptor")

  val host = "localhost"

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
  def get(tokens:Array[String]): String = kvStore getOrElse (tokens(0), "false") // This is problematic, because someone might want to store the string "false"
  
  def migrate(tokens:Array[String]): Unit = {
    // Establish low end of hash range
    val lowHashValue = tokens(0).toInt
    // Establish high end of hash range
    val highHashValue = tokens(1).toInt
    // Establish seed for hash function
    val seed = tokens(2).toInt
    // Establish port of new server
    val newServerPort = tokens(3).toInt

    // Open connection to the new server
    val newServerSock = new Socket(host, newServerPort)
    val newServerIS = new BufferedReader(new InputStreamReader(newServerSock.getInputStream()))
    val newServerPS = new PrintStream(newServerSock.getOutputStream())

    // Loop through all KVPs in kvStore
    for((key, value) <- kvStore) {
      // For each KVP
        // If the key falls within a certain hash value range,
        val keyHashValue = MurmurHash3.stringHash(key, seed)
        if (keyHashValue <= highHashValue && keyHashValue > lowHashValue) {
          // Send the key and value to the new server
          newServerPS.println("set " + key + " " + value)
          // Wait for confirmation from the new server
          val confirmation = newServerIS.readLine // Blocking call?
          // If the confirmation is false, return false; else delete the KVP from the server
          if (confirmation == "true") {
            val args = new Array[String](1)
            args(0) = key
            delete(args)
          }
        }
    }

    // Close the server socket
    newServerSock.close()

  }



  // Adds a new key-value pair to kvStore
  def set(tokens:Array[String]): String = {
    kvStore(tokens(0)) = tokens(1)

    if (kvStore contains tokens(0)) {
      ////////////////////////////////////////////////////////////////
      println("Added key " + tokens(0) + " and value " + tokens(1)) // Prints to terminal for debugging
      ////////////////////////////////////////////////////////////////
      return "true"
    }

    "false"

  }
  
  def switchboard(request:String): String = {
    val tokens = request.split(" ")
    val command = tokens(0)
    command match {
      case "delete"   => delete(tokens.slice(1,3)); return "true"
      case "get"      => return get(tokens.slice(1,2))
      // case "migrate"  => migrate(tokens.slice(1,4)); return "true"
      case "set"      => return set(tokens.slice(1,3))
      case other      => return "Command not found."
    }
    
  }
    
}
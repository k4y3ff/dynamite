import akka.actor.ActorSystem
import akka.actor.ActorDSL._ 

import collection.mutable

import java.net.{ InetSocketAddress, ServerSocket, Socket }
import java.io.{ BufferedReader, InputStreamReader, PrintStream }

import scala.util.hashing.MurmurHash3
import scala.util.{ Try, Success, Failure }

object server1 {

  implicit val coordinatorSystem = ActorSystem("coodinatoracceptor")
  implicit val terminalSystem = ActorSystem("terminalinputreader")

  val host = "localhost"

  val kvStore = collection.mutable.Map[String, String]()
  
  def main(args: Array[String]): Unit = {
    case class Coordinator(sock:Socket, is:BufferedReader, ps:PrintStream, name:String)
    
    val coordinators = new mutable.ArrayBuffer[Coordinator] with mutable.SynchronizedBuffer[Coordinator] {}

    val ss = new ServerSocket(4010)
    
    val coordinatorAcceptor = actor(coordinatorSystem)(new Act {
      become {
        case true => {
          while(true) {
            val sock = ss.accept()
            val is = new BufferedReader(new InputStreamReader(sock.getInputStream()))
            val ps = new PrintStream(sock.getOutputStream())

            // This is what happens whenever a new coordinator connects with the server.
            val coordinatorAdder = actor(coordinatorSystem)(new Act {
              become {
                case true => coordinators += Coordinator(sock, is, ps, (coordinators.length + 1).toString)
              }
            })

            coordinatorAdder ! true
          }
        }
      }
    })

    val coordinatorInputReader = actor(coordinatorSystem)(new Act {
      become {
        case true => {
          // Things to do whenever a client sends something to the server.
          while (true) {
            for (coordinator <- coordinators) {
              if (coordinator.is.ready) {
                val request = coordinator.is.readLine
                coordinator.ps.println(switchboard(request))
              }
            }
          }
        }
      }
    })

    val terminalInputReader = actor(terminalSystem)(new Act {
      become {
        case true => {
          // Things to do whenever someone types a command into the server's terminal window
          while (true) {
            val terminalInput = readLine
            if (terminalInput == "disconnect") {
              coordinatorSystem.shutdown
              ss.close()


              println("Server offline. \n")
            }
            else if (terminalInput == "connect") {
              coordinatorAcceptor ! true
              coordinatorInputReader ! true

              println("Server online. \n")
            }
          }
        }
      }
    })

    coordinatorAcceptor ! true
    coordinatorInputReader ! true
    terminalInputReader ! true

  }
  
  // Removes a single KVP from kvStore
  def delete(tokens:Array[String]): Unit = {
    kvStore.remove(tokens(0))

    if ((kvStore contains tokens(0)) == false) {
      ///////////////////////////////////////////////////////////
      println("Key '" + tokens(0) + "'' deleted from server.") // Prints to terminal for debugging
      ///////////////////////////////////////////////////////////
    }
  }

  // Returns a value, given a key
  def get(tokens:Array[String]): String = {
    val value = kvStore getOrElse (tokens(0), "false") // This is problematic, because someone might want to store the string "false"

    ////////////////////////////////////////////////////////////
    println("KVP '" + tokens(0) + "' retrieved from server.") // Prints to terminal for debugging
    ////////////////////////////////////////////////////////////

    value
  }
  
  def migrate(tokens:Array[String]): Unit = {
    // Establish low end of hash range
    val lowHashValueStr = tokens(0)
    // Establish high end of hash range
    val highHashValueStr = tokens(1)
    // Establish seed for hash function
    val seed = tokens(2).toInt
    // Establish port of new server
    val newServerPort = tokens(3).toInt

    // Open connection to the new server
    
    val newServerSock = new Socket()

    Try(newServerSock.connect(new InetSocketAddress(host, newServerPort), 5000)) match {
      case Success(_) => migrateKVPs()
      case Failure(_) => // NEED TO HAVE AN ACTUAL FAILURE CASE HERE
    }

    def migrateKVPs() {
      val newServerIS = new BufferedReader(new InputStreamReader(newServerSock.getInputStream()))
      val newServerPS = new PrintStream(newServerSock.getOutputStream())

      if (highHashValueStr == "end") { // SHOULD BE USING PATTERN MATCHING!
        val lowHashValue = lowHashValueStr.toInt

        for((key, value) <- kvStore) {
          val keyHashValue = MurmurHash3.stringHash(key, seed)
          
          if (keyHashValue > lowHashValue) {
            newServerPS.println("set " + key + " " + value)
            
            val confirmation = newServerIS.readLine // Blocking call == bad?
            
            if (confirmation == "true") {
              val args = new Array[String](1)
              args(0) = key
              delete(args)
            }
          }

        }
      }

      else if (lowHashValueStr == "beginning") {
        val highHashValue = highHashValueStr.toInt

        for ((key, value) <- kvStore) {
          val keyHashValue = MurmurHash3.stringHash(key, seed)
          
          if (keyHashValue <= highHashValue) {
            newServerPS.println("set " + key + " " + value)
            
            val confirmation = newServerIS.readLine
            
            if (confirmation == "true") {
              val args = new Array[String](1)
              args(0) = key
              delete(args)
            }
          }
        }
      }

      else {
        val lowHashValue = lowHashValueStr.toInt
        val highHashValue = highHashValueStr.toInt

        for((key, value) <- kvStore) {
            val keyHashValue = MurmurHash3.stringHash(key, seed)

            if (keyHashValue <= highHashValue && keyHashValue > lowHashValue) {
              newServerPS.println("set " + key + " " + value)

              val confirmation = newServerIS.readLine

              if (confirmation == "true") {
                val args = new Array[String](1)
                args(0) = key
                delete(args)
              }
            }
        }
      }

      // Close the server socket
      newServerSock.close()
    }
  }

  // Adds a new key-value pair to kvStore
  def set(tokens:Array[String]): String = {
    kvStore(tokens(0)) = tokens(1)

    if (kvStore contains tokens(0)) {
      ////////////////////////////////////////////
      println("Added key '" + tokens(0) + "'.") // Prints to terminal for debugging
      ////////////////////////////////////////////
      return "true"
    }

    "false"

  }
  
  def switchboard(request:String): String = {
    val tokens = request.split(" ")
    val command = tokens(0)
    command match {
      case "countKVPs"  => kvStore.size.toString
      case "delete"     => delete(tokens.slice(1,3)); "true"
      case "get"        => get(tokens.slice(1,2))
      case "migrate"    => migrate(tokens.slice(1,5)); "true"
      case "set"        => set(tokens.slice(1,3))
      case other        => "Command not found."
    } 
  }
    
}
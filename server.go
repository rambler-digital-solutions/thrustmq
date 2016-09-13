package main

import (
  "net"
  "fmt"
  "bufio"
  "os"
)

func interact(connection net.Conn, log *os.File) {
  reader := bufio.NewReader(connection)
  for {
    message, err := reader.ReadString('\n')
    if err != nil {
       connection.Close()
       return
   }
    //fmt.Print("Message Received: ", string(message))
    if _, err := log.WriteString(message); err != nil {
         panic(err)
     }
    connection.Write([]byte("y"))
  }
}


func main() {

  fmt.Println("Launching publisher backend...")

  publisherSocket, _ := net.Listen("tcp", ":1888")
  log, err := os.Create("thrust-queue.txt")
  if err != nil {
       panic(err)
   }

   for {
     connection, _ := publisherSocket.Accept()
     go interact(connection, log)
     fmt.Println("Got a connection, interacting in background...")
   }

}

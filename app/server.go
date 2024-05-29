package main

import (
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		// Buffer to store data
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			return
		}
		// Respond with PONG
		conn.Write([]byte("+PONG\r\n"))
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Listening on port 6379...")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn) // Handle each connection in a new goroutine
	}
}

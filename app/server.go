package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
)

// RESP format constants
const (
	respSimpleString = "+"
	respBulkString   = "$"
	respError        = "-"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		// Read the input command
		input, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			return
		}

		// Remove the trailing newline and carriage return
		input = bytes.TrimSpace(input)

		// Split the command and its arguments
		parts := strings.SplitN(string(input), " ", 2)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PING":
			conn.Write([]byte(respSimpleString + "PONG\r\n"))
		case "ECHO":
			if len(parts) < 2 {
				conn.Write([]byte(respError + "ERR wrong number of arguments for 'echo' command\r\n"))
			} else {
				message := parts[1]
				resp := fmt.Sprintf("%s%d\r\n%s\r\n", respBulkString, len(message), message)
				conn.Write([]byte(resp))
			}
		default:
			conn.Write([]byte(respError + "ERR unknown command\r\n"))
		}
	}
}

func main() {
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

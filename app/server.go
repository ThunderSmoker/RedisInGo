package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KVPair struct {
	value     string
	expiry    time.Time
	hasExpiry bool
}

var kvStore = make(map[string]KVPair)
var mu sync.Mutex

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

		// Check if the input is an array
		if input[0] == '*' {
			// Read the number of elements
			elements, err := strconv.Atoi(strings.TrimSpace(string(input[1:])))
			if err != nil {
				fmt.Println("Error parsing number of elements:", err.Error())
				return
			}

			// Parse the command and arguments
			var cmdParts []string
			for i := 0; i < elements; i++ {
				// Read the bulk string header
				header, err := reader.ReadBytes('\n')
				if err != nil {
					fmt.Println("Error reading bulk string header:", err.Error())
					return
				}

				// Read the length of the bulk string
				if header[0] != '$' {
					fmt.Println("Expected '$' in bulk string header")
					return
				}
				length, err := strconv.Atoi(strings.TrimSpace(string(header[1:])))
				if err != nil {
					fmt.Println("Error parsing bulk string length:", err.Error())
					return
				}

				// Read the bulk string itself
				data := make([]byte, length)
				_, err = reader.Read(data)
				if err != nil {
					fmt.Println("Error reading bulk string data:", err.Error())
					return
				}

				// Read the trailing newline
				_, err = reader.ReadBytes('\n')
				if err != nil {
					fmt.Println("Error reading trailing newline:", err.Error())
					return
				}

				cmdParts = append(cmdParts, string(data))
			}

			// Handle the parsed command
			if len(cmdParts) > 0 {
				cmd := strings.ToUpper(cmdParts[0])
				switch cmd {
				case "PING":
					conn.Write([]byte("+PONG\r\n"))
				case "ECHO":
					if len(cmdParts) < 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
					} else {
						message := cmdParts[1]
						resp := fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)
						conn.Write([]byte(resp))
					}
				case "SET":
					if len(cmdParts) < 3 {
						conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					} else {
						key := cmdParts[1]
						value := cmdParts[2]
						var expiry time.Time
						var hasExpiry bool
						if len(cmdParts) > 3 {
							option := strings.ToUpper(cmdParts[3])
							if option == "PX" && len(cmdParts) == 5 {
								expiryMillis, err := strconv.Atoi(cmdParts[4])
								if err != nil {
									conn.Write([]byte("-ERR invalid PX value\r\n"))
									return
								}
								expiry = time.Now().Add(time.Duration(expiryMillis) * time.Millisecond)
								hasExpiry = true
							} else {
								conn.Write([]byte("-ERR syntax error\r\n"))
								return
							}
						}
						mu.Lock()
						kvStore[key] = KVPair{value: value, expiry: expiry, hasExpiry: hasExpiry}
						mu.Unlock()
						conn.Write([]byte("+OK\r\n"))
					}
				case "GET":
					if len(cmdParts) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
					} else {
						key := cmdParts[1]
						mu.Lock()
						pair, exists := kvStore[key]
						if exists && pair.hasExpiry && time.Now().After(pair.expiry) {
							// Key has expired
							delete(kvStore, key)
							exists = false
						}
						mu.Unlock()
						if exists {
							resp := fmt.Sprintf("$%d\r\n%s\r\n", len(pair.value), pair.value)
							conn.Write([]byte(resp))
						} else {
							conn.Write([]byte("$-1\r\n"))
						}
					}
				default:
					conn.Write([]byte("-ERR unknown command\r\n"))
				}
			}
		} else {
			conn.Write([]byte("-ERR protocol error\r\n"))
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

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			now := time.Now()
			mu.Lock()
			for key, pair := range kvStore {
				if pair.hasExpiry && now.After(pair.expiry) {
					delete(kvStore, key)
				}
			}
			mu.Unlock()
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn) // Handle each connection in a new goroutine
	}
}

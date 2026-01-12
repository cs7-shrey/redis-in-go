package main

import (
	"bufio"
	"log"
	"net"
	"server/resp"
	"server/commands"
)


func main() {
	server, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal("Error starting server", err)
	}

	defer server.Close()

	for {
		conn, err := server.Accept()
		if err != nil {
			log.Println("Error accepting connection ", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	executor := commands.NewExecutor()

	message, err := parser.Parse();
	if err != nil {
		return;
	}

	// commands := executor.ParseCommands(message)


}
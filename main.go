package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"server/commands"
	"server/resp"
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

	for {
		message, err := parser.Parse();

		if err != nil && err.Error() == "INVALID DATA TYPE" {
			conn.Write(executor.GetErrorBytes(err.Error()))
			continue
		} else if err != nil && err == io.EOF{
			return;
		} else if err != nil {
			conn.Write(executor.GetErrorBytes(err.Error()))
			continue;
		}

		cmd, err := executor.ParseCommand(message)

		if err != nil {
			conn.Write(executor.GetErrorBytes("ERR COULD NOT EXECUTE COMMAND"))
			continue
		}

		response := executor.ExecuteCommand(cmd)

		if response == nil {
			conn.Write(executor.GetErrorBytes("ERR COULD NOT EXECUTE COMMAND"))
			continue
		}
		conn.Write(response)
	}
}
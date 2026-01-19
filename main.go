package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"server/commands/executor"
	"server/commands/serializer"
	"server/errs"
	"server/resp"
	"server/store"
	"server/store/cleanup"
)


func main() {
	server, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal("Error starting server", err)
	}

	defer server.Close()

	store := store.NewStore()
	executor := executor.NewExecutor(store)
	go cleanup.RunCleanup(store)

	for {
		conn, err := server.Accept()
		if err != nil {
			log.Println("Error accepting connection ", err)
			continue
		}
		
		go handleConnection(conn, executor)
	}
}


func handleConnection(conn net.Conn, executor *executor.Executor) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	sr := serializer.NewSerializer()

	for {
		message, err := parser.Parse();

		fmt.Println(message)

		if err != nil && err == errs.InvalidDataType{
			conn.Write(sr.GetErrorBytes(err.Error()))
			continue
		} else if err != nil && err == io.EOF{
			return;
		} else if err != nil {
			conn.Write(sr.GetErrorBytes(err.Error()))
			continue;
		}

		cmd, err := executor.ParseCommand(message)

		fmt.Println(cmd)

		if err != nil {
			conn.Write(sr.GetErrorBytes("ERR COULD NOT EXECUTE COMMAND"))
			continue
		}

		response := executor.ExecuteCommand(cmd)

		if response == nil {
			conn.Write(sr.GetErrorBytes("ERR COULD NOT EXECUTE COMMAND"))
			continue
		}
		conn.Write(response)
	}
}
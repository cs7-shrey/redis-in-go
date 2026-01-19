package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"server/commands/executor"
	"server/commands/serializer"
	"server/errs"
	"server/persistence/aof"
	"server/resp"
	"server/store"
	"server/store/actions"
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

	replayAof(executor)

	aof.StartAof()
	go cleanup.RunCleanup(store)

	fmt.Println("Accepting connections at port 8080")

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
		if _, ok := actions.MutationCommands[cmd.Action]; ok && response[0] != '-' {
			aof.AofChan <- cmd
		}

		if response == nil {
			conn.Write(sr.GetErrorBytes("ERR COULD NOT EXECUTE COMMAND"))
			continue
		}
		conn.Write(response)
	}
}

func replayAof(executor *executor.Executor) error {
	fmt.Println("Started AOF replay")

	file, err := os.Open("appendonly.aof")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	
	reader := bufio.NewReader(file)
	parser := resp.NewParser(reader)

	for {
		message, err := parser.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("AOF parse error: %w", err)
		}

		cmd, err := executor.ParseCommand(message)
		if err != nil {
			return fmt.Errorf("AOF command error: %w", err)
		}

		executor.ExecuteCommand(cmd)
	}

	return nil
}
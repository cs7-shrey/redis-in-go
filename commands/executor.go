package commands

import (
	"errors"
	"fmt"
	"strings"
)

type Executor struct {}

type RedisCommand struct {
	Command string
	Arguments []string
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) ParseCommand(message any) (*RedisCommand, error){
	switch message := message.(type) {

	case string:
		return &RedisCommand{
			Command: message,
		}, nil
	
	case []string: 
		return &RedisCommand{
			Command: message[0],
			Arguments: message[1:],
		}, nil
			
	default: 
		return nil, errors.New("invalid type")
	}
}

func (e *Executor) ExecuteCommand(cmd *RedisCommand) []byte{
	switch strings.ToLower(cmd.Command) {
		case "ping":
			return e.getSimpleStringBytes("PONG")
		
		default:
			return e.GetErrorBytes("ERR unknown command")
	}
}

func (e *Executor) GetErrorBytes(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

func (e *Executor) getSimpleStringBytes(s string)[] byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func (e *Executor) getBulkStringBytes(s string) []byte {
	length := len(s);
	bulkString := fmt.Sprintf("$%d\r\n%s", length, s)
	return []byte(bulkString)
}
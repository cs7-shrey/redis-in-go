package commands

import (
	"errors"
	"fmt"
	"server/errs"
	"server/store"
)

type Executor struct {
	store *store.Store
}

type Action string

const (
	ping   Action = "ping"
	echo   Action = "echo"
	get    Action = "get"
	set    Action = "set"
	del    Action = "del"
	exists Action = "exists"
)

type RedisCommand struct {
	Action    Action
	Arguments []string
}

func NewExecutor(store *store.Store) *Executor {
	return &Executor{
		store: store,
	}
}

func (e *Executor) ParseCommand(msg any) (*RedisCommand, error) {
	switch msg := msg.(type) {

	case string:
		action, err := e.validateCommandExistence(msg)
		if err != nil {
			return nil, err
		}

		return &RedisCommand{
			Action: action,
		}, nil

	case []any:
		if len(msg) == 0 {
			return nil, errors.New("empty command")
		}

		cmd, ok := msg[0].(string)
		if !ok {
			return nil, errors.New("command name must be string")
		}

		action, err := e.validateCommandExistence(cmd)

		args := make([]string, len(msg) - 1)
		for i := 1; i < len(msg); i++ {
			s, ok := msg[i].(string)
			if !ok {
				return nil, errors.New("arguments must be strings")
			}
			args[i-1] = s
		}

		// fmt.Println(args)

		if err != nil {
			return nil, err
		}

		return &RedisCommand{
			Action:    action,
			Arguments: args,
		}, nil

	default:
		return nil, errors.New("invalid type")
	}
}

func (e *Executor) ExecuteCommand(cmd *RedisCommand) []byte {
	err := e.validateCommandArgs(cmd)

	if err != nil {
		return e.GetErrorBytes(err.Error())
	}

	switch cmd.Action {
	case ping:
		return e.getSimpleStringBytes("PONG")

	case echo:
		return e.getSimpleStringBytes(cmd.Arguments[0])

	case get:
		value, err := e.store.Get(cmd.Arguments[0])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getSimpleStringBytes(value)

	case set:
		e.store.Set(cmd.Arguments[0], cmd.Arguments[1])
		return e.getSimpleStringBytes("OK")

	case del:
		n := e.store.Delete(cmd.Arguments)
		response := fmt.Sprintf("%d", n)
		return e.getSimpleStringBytes(response)

	case exists:
		n := e.store.Exists(cmd.Arguments)
		response := fmt.Sprintf("%d", n)
		return e.getSimpleStringBytes(response)

	default:
		return e.GetErrorBytes("ERR unknown command")
	}
}

func (e *Executor) GetErrorBytes(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

func (e *Executor) getSimpleStringBytes(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func (e *Executor) getBulkStringBytes(s string) []byte {
	length := len(s)
	bulkString := fmt.Sprintf("$%d\r\n%s", length, s)
	return []byte(bulkString)
}

func (e *Executor) validateCommandExistence(command string) (Action, error) {
	switch Action(command) {
	case ping, get, set, del, exists, echo:
		return Action(command), nil

	default:
		return "", errs.InvalidCommand
	}
}

func (e *Executor) validateCommandArgs(cmd *RedisCommand) error {
	switch cmd.Action {
	case ping:
		if len(cmd.Arguments) != 0 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case get, echo:
		if len(cmd.Arguments) < 1 || len(cmd.Arguments) > 2 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case del, exists:
		if len(cmd.Arguments) < 1 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case set:
		if len(cmd.Arguments) < 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil

	default:
		return errs.InvalidCommand
	}
}

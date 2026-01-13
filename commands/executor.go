package commands

import (
	"errors"
	"fmt"
	"server/errs"
	"server/store"
	"server/store/actions"
)

type Executor struct {
	store *store.Store
}

type RedisCommand struct {
	Action    actions.Action
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
	case actions.Ping:
		return e.getSimpleStringBytes("PONG")

	case actions.Echo:
		return e.getSimpleStringBytes(cmd.Arguments[0])

	case actions.Get:
		value, err := e.store.Get(cmd.Arguments[0])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getSimpleStringBytes(value)

	case actions.Set:
		e.store.Set(cmd.Arguments[0], cmd.Arguments[1])
		return e.getSimpleStringBytes("OK")

	case actions.Del:
		n := e.store.Delete(cmd.Arguments)
		response := fmt.Sprintf("%d", n)
		return e.getSimpleStringBytes(response)

	case actions.Exists:
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

func (e *Executor) validateCommandExistence(command string) (actions.Action, error) {
	switch actions.Action(command) {
	case actions.Ping, actions.Get, actions.Set, actions.Del, actions.Exists, actions.Echo:
		return actions.Action(command), nil

	default:
		return "", errs.InvalidCommand
	}
}

func (e *Executor) validateCommandArgs(cmd *RedisCommand) error {
	switch cmd.Action {
	case actions.Ping:
		if len(cmd.Arguments) != 0 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case actions.Get, actions.Echo:
		if len(cmd.Arguments) < 1 || len(cmd.Arguments) > 2 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case actions.Del, actions.Exists:
		if len(cmd.Arguments) < 1 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case actions.Set:
		if len(cmd.Arguments) < 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil

	default:
		return errs.InvalidCommand
	}
}

package commands

import (
	"errors"
	"fmt"
	"server/errs"
	"server/store"
	"server/store/actions"
	"strconv"
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

		args := make([]string, len(msg)-1)
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
		if err == errs.ErrNotFound {
			return e.GetNil()
		}

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

	case actions.Expire:
		expiry_seconds, err := strconv.Atoi(cmd.Arguments[1])
		if err != nil || expiry_seconds <= 0{
			return e.GetErrorBytes("TIME IS NOT A POSITIVE INTEGER")
		}

		err = e.store.Expire(cmd.Arguments[0], int64(expiry_seconds))
		if err != nil {
			return e.getIntegerBytes(0)
		}

		return e.getIntegerBytes(1)
	
	case actions.TTL:
		ttl := e.store.TTL(cmd.Arguments[0])
		return e.getIntegerBytes(ttl)

	case actions.LPush:
		n, err := e.store.LPush(cmd.Arguments[0], cmd.Arguments[1:])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		response := fmt.Sprintf("%d", n)
		return e.getSimpleStringBytes(response)

	case actions.LPop:
		count := 1
		if len(cmd.Arguments) == 2 {
			count, err = strconv.Atoi(cmd.Arguments[1])
			if err != nil || count <= 0 {
				return e.GetErrorBytes("COUNT MUST BE A POSITIVE INTEGER")
			}
		}

		items, err := e.store.LPop(cmd.Arguments[0], count)
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getArrayOfBulkStringBytes(items)

	case actions.RPush:
		n, err := e.store.RPush(cmd.Arguments[0], cmd.Arguments[1:])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		response := fmt.Sprintf("%d", n)
		return e.getSimpleStringBytes(response)

	case actions.RPop:
		count := 1
		if len(cmd.Arguments) == 2 {
			count, err = strconv.Atoi(cmd.Arguments[1])
			if err != nil || count <= 0 {
				return e.GetErrorBytes("COUNT MUST BE A POSITIVE INTEGER")
			}
		}
		
		items, err := e.store.RPop(cmd.Arguments[0], count)
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getArrayOfBulkStringBytes(items)

	case actions.BLPop:
		item, err := e.store.BLPop(cmd.Arguments[0])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getBulkStringBytes(item)

	case actions.BRPop:
		item, err := e.store.BRPop(cmd.Arguments[0])
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		return e.getBulkStringBytes(item)


	// ———————————————————————————————————————————————————————————————
	// Hash set commands
	// ———————————————————————————————————————————————————————————————
	
	case actions.HGet:
		value, err := e.store.HGet(cmd.Arguments[0], cmd.Arguments[1])
		if err == errs.ErrNotFound {
			return e.GetNil()
		}
		
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}

		return e.getBulkStringBytes(value)
	
	case actions.HSet:
		cnt, err := e.store.HSet(cmd.Arguments[0], cmd.Arguments[1:])
		
		if err != nil {
			return e.GetErrorBytes(err.Error())
		}

		return e.getIntegerBytes(cnt)
	
	case actions.HGetAll:
		response, err := e.store.HGetAll(cmd.Arguments[0])

		if err == errs.ErrNotFound {
			return e.getArrayOfBulkStringBytes([]string{})
		}

		if err != nil {
			return e.GetErrorBytes(err.Error())
		}
		
		return e.getArrayOfBulkStringBytes(response)
	
	case actions.HDel:
		cnt, err := e.store.HDel(cmd.Arguments[0], cmd.Arguments[1:])

		if err == errs.ErrNotFound {
			return e.getIntegerBytes(0)
		}

		if err != nil {
			return e.GetErrorBytes(err.Error())
		}

		return e.getIntegerBytes(cnt)

	default:
		return e.GetErrorBytes("ERR unknown command")
	}
}

func (e *Executor) GetNil() []byte {
	return []byte("$-1\r\n")
}

func (e *Executor) GetErrorBytes(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

func (e *Executor) getSimpleStringBytes(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func (e *Executor) getIntegerBytes(i int) []byte {
	extra := 0
	if i < 0 { extra = 1}

	size := 1 + extra + 20 + 2
	buf := make([]byte, 0, size)

	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, int64(i), 10)
	buf = append(buf, '\r')
	buf = append(buf, '\n')

	return buf
}

func (e *Executor) getBulkStringBytes(s string) []byte {
	data := []byte(s)

	// preallocate: $ length(20 decimal places) \r\n data \r\n
	size := e.getBulkStringBytesSize(s)
	buf := make([]byte, 0, size)

	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(data)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, data...)
	buf = append(buf, '\r', '\n')

	return buf
}

func (e *Executor) getArrayOfBulkStringBytes(items []string) []byte {
	// * length(20 decimal) \r\n data
	capEst := 1 + 20 + 2

	for _, item := range items {
		capEst += e.getBulkStringBytesSize(item)
	}

	buf := make([]byte, 0, capEst)

	// array header
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(items)), 10)
	buf = append(buf, '\r', '\n')

	for _, item := range items {
		buf = append(buf, e.getBulkStringBytes(item)...)
	}

	return buf
}

func (e *Executor) getBulkStringBytesSize(s string) int {
	// $ length(20 decimal places) \r\n len(s) \r\n
	return 1+ 20 + 2 + len(s) + 2
}

func (e *Executor) validateCommandExistence(command string) (actions.Action, error) {
	action := actions.Action(command)

	if _, ok := actions.ValidCommands[action]; ok {
		return action, nil
	} else {
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

	// GET key, hgetall key
	case actions.Get, actions.Echo, actions.TTL, actions.HGetAll:
		if len(cmd.Arguments) != 1 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	case actions.Del, actions.Exists:
		if len(cmd.Arguments) < 1 {
			return errs.IncorrectNumberOfArguments
		}

		return nil

	// hdel key field [field...]
	case actions.HDel:
		if len(cmd.Arguments) < 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil
	
	// set key value expire key seconds, hget key field
	case actions.Set, actions.Expire, actions.HGet:
		if len(cmd.Arguments) != 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil

	// list, count or list
	case actions.LPop, actions.RPop:
		if len(cmd.Arguments) != 1 && len(cmd.Arguments) != 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil

	case actions.LPush, actions.RPush:
		if len(cmd.Arguments) < 2 {
			return errs.IncorrectNumberOfArguments
		}
		return nil
	
	case actions.BLPop, actions.BRPop:
		if len(cmd.Arguments) != 1 {
			return errs.IncorrectNumberOfArguments
		}
		return nil
	
	// HSET key field value [field value ...]
	case actions.HSet:
		if len(cmd.Arguments) < 3 || len(cmd.Arguments) & 1 == 0 {
			return errs.IncorrectNumberOfArguments
		}
		return nil

	default:
		return errs.InvalidCommand
	}
}

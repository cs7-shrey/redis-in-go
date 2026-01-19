package executor

import (
	"errors"
	"fmt"
	"server/commands"
	"server/commands/serializer"
	"server/errs"
	"server/store"
	"server/store/actions"
	"strconv"
)

type Executor struct {
	store *store.Store
	sr *serializer.Serializer
}

func NewExecutor(store *store.Store) *Executor {
	return &Executor{
		store: store,
		sr: serializer.NewSerializer(),
	}
}

func (e *Executor) ParseCommand(msg any) (*commands.RedisCommand, error) {
	switch msg := msg.(type) {

	case string:
		action, err := e.validateCommandExistence(msg)
		if err != nil {
			return nil, err
		}

		return &commands.RedisCommand{
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

		return &commands.RedisCommand{
			Action:    action,
			Arguments: args,
		}, nil

	default:
		return nil, errors.New("invalid type")
	}
}

func (e *Executor) ExecuteCommand(cmd *commands.RedisCommand) []byte {
	err := e.validateCommandArgs(cmd)

	if err != nil {
		return e.sr.GetErrorBytes(err.Error())
	}

	switch cmd.Action {
	case actions.Ping:
		return e.sr.GetSimpleStringBytes("PONG")

	case actions.Echo:
		return e.sr.GetSimpleStringBytes(cmd.Arguments[0])

	case actions.Get:
		value, err := e.store.Get(cmd.Arguments[0])
		if err == errs.ErrNotFound {
			return e.sr.GetNil()
		}

		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		return e.sr.GetSimpleStringBytes(value)

	case actions.Set:
		e.store.Set(cmd.Arguments[0], cmd.Arguments[1])
		return e.sr.GetSimpleStringBytes("OK")

	case actions.Del:
		n := e.store.Delete(cmd.Arguments)
		response := fmt.Sprintf("%d", n)
		return e.sr.GetSimpleStringBytes(response)

	case actions.Exists:
		n := e.store.Exists(cmd.Arguments)
		response := fmt.Sprintf("%d", n)
		return e.sr.GetSimpleStringBytes(response)

	case actions.Expire:
		expiry_seconds, err := strconv.Atoi(cmd.Arguments[1])
		if err != nil || expiry_seconds <= 0{
			return e.sr.GetErrorBytes("TIME IS NOT A POSITIVE INTEGER")
		}

		err = e.store.Expire(cmd.Arguments[0], int64(expiry_seconds))
		if err != nil {
			return e.sr.GetIntegerBytes(0)
		}

		return e.sr.GetIntegerBytes(1)
	
	case actions.TTL:
		ttl := e.store.TTL(cmd.Arguments[0])
		return e.sr.GetIntegerBytes(ttl)

	case actions.LPush:
		n, err := e.store.LPush(cmd.Arguments[0], cmd.Arguments[1:])
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		response := fmt.Sprintf("%d", n)
		return e.sr.GetSimpleStringBytes(response)

	case actions.LPop:
		count := 1
		if len(cmd.Arguments) == 2 {
			count, err = strconv.Atoi(cmd.Arguments[1])
			if err != nil || count <= 0 {
				return e.sr.GetErrorBytes("COUNT MUST BE A POSITIVE INTEGER")
			}
		}

		items, err := e.store.LPop(cmd.Arguments[0], count)
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		return e.sr.GetArrayOfBulkStringBytes(items)

	case actions.RPush:
		n, err := e.store.RPush(cmd.Arguments[0], cmd.Arguments[1:])
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		response := fmt.Sprintf("%d", n)
		return e.sr.GetSimpleStringBytes(response)

	case actions.RPop:
		count := 1
		if len(cmd.Arguments) == 2 {
			count, err = strconv.Atoi(cmd.Arguments[1])
			if err != nil || count <= 0 {
				return e.sr.GetErrorBytes("COUNT MUST BE A POSITIVE INTEGER")
			}
		}
		
		items, err := e.store.RPop(cmd.Arguments[0], count)
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		return e.sr.GetArrayOfBulkStringBytes(items)

	case actions.BLPop:
		item, err := e.store.BLPop(cmd.Arguments[0])
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		return e.sr.GetBulkStringBytes(item)

	case actions.BRPop:
		item, err := e.store.BRPop(cmd.Arguments[0])
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		return e.sr.GetBulkStringBytes(item)


	// ———————————————————————————————————————————————————————————————
	// Hash set commands
	// ———————————————————————————————————————————————————————————————
	
	case actions.HGet:
		value, err := e.store.HGet(cmd.Arguments[0], cmd.Arguments[1])
		if err == errs.ErrNotFound {
			return e.sr.GetNil()
		}
		
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}

		return e.sr.GetBulkStringBytes(value)
	
	case actions.HSet:
		cnt, err := e.store.HSet(cmd.Arguments[0], cmd.Arguments[1:])
		
		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}

		return e.sr.GetIntegerBytes(cnt)
	
	case actions.HGetAll:
		response, err := e.store.HGetAll(cmd.Arguments[0])

		if err == errs.ErrNotFound {
			return e.sr.GetArrayOfBulkStringBytes([]string{})
		}

		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}
		
		return e.sr.GetArrayOfBulkStringBytes(response)
	
	case actions.HDel:
		cnt, err := e.store.HDel(cmd.Arguments[0], cmd.Arguments[1:])

		if err == errs.ErrNotFound {
			return e.sr.GetIntegerBytes(0)
		}

		if err != nil {
			return e.sr.GetErrorBytes(err.Error())
		}

		return e.sr.GetIntegerBytes(cnt)

	default:
		return e.sr.GetErrorBytes("ERR unknown command")
	}
}


func (e *Executor) validateCommandExistence(command string) (actions.Action, error) {
	action := actions.Action(command)

	if _, ok := actions.ValidCommands[action]; ok {
		return action, nil
	} else {
		return "", errs.InvalidCommand
	}
}

func (e *Executor) validateCommandArgs(cmd *commands.RedisCommand) error {
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

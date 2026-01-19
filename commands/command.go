package commands

import "server/store/actions"

type RedisCommand struct {
	Action    actions.Action
	Arguments []string
}

package actions

type Action string

const (
	Ping   Action = "ping"
	Echo   Action = "echo"
	Get    Action = "get"
	Set    Action = "set"
	Del    Action = "del"
	Exists Action = "exists"
	Expire Action = "expire"
	TTL    Action = "ttl"

	LPush Action = "lpush"
	LPop  Action = "lpop"
	RPush Action = "rpush"
	RPop  Action = "rpop"
	BLPop Action = "blpop"
	BRPop Action = "brpop"

	HGet    Action = "hget"
	HSet    Action = "hset"
	HGetAll Action = "hgetall"
	HDel    Action = "hdel"
)

var ValidCommands = map[Action]struct{}{
	Ping:   {},
	Get:    {},
	Set:    {},
	Del:    {},
	Exists: {},
	Expire: {},
	TTL:    {},
	Echo:   {},

	LPop:  {},
	LPush: {},
	RPush: {},
	RPop:  {},
	BLPop: {},
	BRPop: {},

	HSet:    {},
	HGet:    {},
	HGetAll: {},
	HDel:    {},
}

type BlockingPopDirection Action

const (
	BLEFT  BlockingPopDirection = "left"
	BRIGHT BlockingPopDirection = "right" // Blocking Right
)

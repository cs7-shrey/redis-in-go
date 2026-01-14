package actions

type Action string

const (
	Ping   Action = "ping"
	Echo   Action = "echo"
	Get    Action = "get"
	Set    Action = "set"
	Del    Action = "del"
	Exists Action = "exists"

	LPush Action = "lpush"
	LPop Action = "lpop"
	RPush Action = "rpush"
	RPop Action = "rpop"
	BLPop Action = "blpop"
	BRPop Action = "brpop"
)

var ValidCommands = map[Action]struct{}{
	Ping:   {},
	Get:    {},
	Set:    {},
	Del:    {},
	Exists: {},
	Echo:   {},
	LPop:   {},
	LPush:  {},
	RPush:  {},
	RPop:   {},
	BLPop: 	{},
	BRPop: 	{},
}

type BlockingPopDirection Action

const (
	BLEFT BlockingPopDirection = "left"
	BRIGHT BlockingPopDirection = "right"			// Blocking Right
)

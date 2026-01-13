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
)
package commands

type Executor struct {}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) ParseCommands(message any) {

}
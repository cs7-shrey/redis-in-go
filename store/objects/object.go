package objects

type DataType string

const (
	String DataType = "string"
	List   DataType = "list"
	Hash DataType = "hash"
	Set DataType = "set"
)

type Object struct {
	DataType DataType
	Data any
}

func NewObject(dataType DataType, data any) *Object {
	return &Object{
		DataType: dataType,
		Data: data,
	}
}
package objects

import "time"

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
	expiry *Expiry
}

type Expiry struct {
	At time.Time
}

func NewObject(dataType DataType, data any) *Object {
	return &Object{
		DataType: dataType,
		Data: data,
	}
}

func (object *Object) HasExpired() bool {
	return object.expiry != nil && time.Now().After(object.expiry.At)
}

func (object *Object) Expire(seconds int64) {
	exp_at := time.Now().Add(time.Duration(seconds) * time.Second)
	if object.expiry != nil {
		object.expiry.At = exp_at
	} else {
		object.expiry = &Expiry{
			At: exp_at,
		}
	}
}

func (object *Object) GetExpiry() *Expiry {
	return object.expiry
}

func (object *Object) TTL() int {
	if object.expiry == nil {
		return -1
	}

	return int(time.Until(object.expiry.At).Seconds())
}
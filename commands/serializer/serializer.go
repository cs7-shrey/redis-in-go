package serializer

import (
	"server/commands"
	"strconv"
)

type Serializer struct {}

func NewSerializer() *Serializer {
	return &Serializer{}
}

func (sr *Serializer) SerializeCommand(cmd *commands.RedisCommand) []byte {
	array := append([]string{string(cmd.Action)}, cmd.Arguments...)
	return sr.GetArrayOfBulkStringBytes(array)
}

func (sr *Serializer) GetNil() []byte {
	return []byte("$-1\r\n")
}

func (sr *Serializer) GetErrorBytes(s string) []byte {
	size := 1 + len(s) + 2
	buf := make([]byte, 0, size)

	buf = append(buf, '-')
	buf = append(buf, s...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')

	return buf
}

func (sr *Serializer) GetSimpleStringBytes(s string) []byte {
	// + string \r\n
	size := 1 + len(s) + 2
	buf := make([]byte, 0, size)

	buf = append(buf, '+')
	buf = append(buf, s...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')

	return buf
}

func (sr *Serializer) GetIntegerBytes(i int) []byte {
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

func (sr *Serializer) GetBulkStringBytes(s string) []byte {
	data := []byte(s)

	// preallocate: $ length(20 decimal places) \r\n data \r\n
	size := sr.getBulkStringBytesSize(s)
	buf := make([]byte, 0, size)

	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(data)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, data...)
	buf = append(buf, '\r', '\n')

	return buf
}

func (sr *Serializer) GetArrayOfBulkStringBytes(items []string) []byte {
	// * length(20 decimal) \r\n data
	capEst := 1 + 20 + 2

	for _, item := range items {
		capEst += sr.getBulkStringBytesSize(item)
	}

	buf := make([]byte, 0, capEst)

	// array header
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(items)), 10)
	buf = append(buf, '\r', '\n')

	for _, item := range items {
		buf = append(buf, sr.GetBulkStringBytes(item)...)
	}

	return buf
}

func (sr *Serializer) getBulkStringBytesSize(s string) int {
	// $ length(20 decimal places) \r\n len(s) \r\n
	return 1+ 20 + 2 + len(s) + 2
}
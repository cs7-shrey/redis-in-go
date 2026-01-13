package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"server/errs"
	"strconv"
	"strings"
)

type Parser struct {
	reader *bufio.Reader;
}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{
		reader: reader,
	}
}

func (parser *Parser) Parse() (any, error) {
	reader := parser.reader;

	typeByte, err := reader.ReadByte()

	if err != nil {
		return nil, err
	}

	// Reading upto \n doesn't matter
	// For having \n inside payload, you can send it inside
	// bulk strings since they read the exact number of bytes
	// without considering the delimiter

	switch typeByte {
		case '+':
			line, err := reader.ReadString('\n')

			if err != nil {
				return nil, err
			}

			if len(line) < 2 || line[len(line) - 2] != '\r' {
				return nil, errors.New("Protocol error")
			}

			return strings.TrimRight(line, "\r\n"), nil
	
		case ':':
			line, _ := reader.ReadString('\n')
			if len(line) < 2 || line[len(line) - 2] != '\r' {
				return nil, errors.New("Protocol error")
			}
			return strconv.Atoi(strings.TrimRight(line, "\r\n"))
		
		case '$':
			line, _ := reader.ReadString('\n')
			if len(line) < 2 || line[len(line) - 2] != '\r' {
				return nil, errors.New("Protocol error")
			}

			length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))

			if err != nil {
				return nil, err
			}

			data := make([]byte, length)
			_, err = io.ReadFull(reader, data)

			if err != nil {
				return nil, err
			}

			// read /r/n
			reader.ReadByte()
			reader.ReadByte()
		
			return string(data), nil

		case '*':
			line, err := reader.ReadString('\n')

			if err != nil {
				return nil, err
			}

			length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))

			if err != nil {
				return nil, err
			}
			
			message := make([]any, length)

			for i := range length {
				message[i], _ = parser.Parse()
			}

			return message, nil
		
		default: 
			fmt.Println("invalid type")
			return nil, errs.InvalidDataType
	}
}

package resp

import (
	"bufio"
	"errors"
	"io"
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

func (parser *Parser) Parse() (interface{}, error) {
	reader := parser.reader;

	typeByte, err := reader.ReadByte()

	if err != nil {
		return nil, err
	}

	switch typeByte {
		case '+':
			line, _ := reader.ReadString('\n')
			return strings.TrimRight(line, "\r\n"), nil
	
		case ':':
			line, _ := reader.ReadString('\n')
			return strconv.Atoi(strings.TrimRight(line, "\r\n"))
		
		case '$':
			line, _ := reader.ReadString('\n')
			
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
			return nil, errors.New("Invalid data type")
	}
}

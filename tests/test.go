package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

func main() {
	data := "Hello, Go readers!\r\n Bantai ko lageli bhook, chup tmkc\r\n"
	reader := strings.NewReader(data)

	br := NewReader(reader)

	res, err := br.ReadUntilCRLF()

	if err != nil {
		panic(err)
	}

	fmt.Printf("%q", res)

	fmt.Printf("------------------------")

	res, err = br.ReadUntilCRLF()

	if err != nil {
		panic(err)
	}

	fmt.Printf("%s", res)
}

// srey\r\n
func readUntilCRLF(r io.Reader) ([]byte, error) {
	buffer := make([]byte, 1)
	result := make([]byte, 0)

	lastByte := byte(0)

	for {
		_, err := r.Read(buffer)

		if err != nil { // EOF or any other error
			return nil, err
		}

		item := buffer[0]
		if item == '\n' && lastByte == '\r' {
			return result[:len(result)-1], nil
		}

		result = append(result, item)
		lastByte = item
	}
}

func readExact(r io.Reader, n int) ([]byte, error) {
	buffer := make([]byte, 1)
	result := make([]byte, n)

	for i := 0; i < n; i++ {
		_, err := r.Read(buffer)

		if err == io.EOF {
			return nil, errors.New("EOF reached before n bytes")
		}

		if err != nil {
			return nil, err
		}

		result[i] = buffer[0]
	}

	return result, nil
}

type BufferedReader struct {
	r      io.Reader
	buffer []byte
	pos    int
	end    int
}

// Note: buffer[pos:end] denotes the unread bytes
func NewReader(r io.Reader) *BufferedReader {
	return &BufferedReader{
		r:      r,
		buffer: make([]byte, 4*1024), // 4 KB
		pos:    0,
		end:    0,
	}
}

func (br *BufferedReader) shiftToStart() {
	copy(br.buffer[0:], br.buffer[br.pos:br.end])
	br.end = br.end - br.pos
	br.pos = 0
}

func (br *BufferedReader) doubleSize() {
	size := len(br.buffer)

	newBuffer := make([]byte, 2*size)
	copy(newBuffer, br.buffer)

	br.buffer = newBuffer
}

// [pos, end) is valid data
func (br *BufferedReader) readIntoBuffer() (int, error) {
	// if buffer has space at end, do nothing
	// else if pos > 0 -> shifting
	// else grow the buffer to twice it's size and then read
	// read data from end:

	if br.end < len(br.buffer) {
	} else if br.pos > 0 {
		br.shiftToStart()
	} else {
		br.doubleSize()
	}

	n, err := br.r.Read(br.buffer[br.end:])
	br.end += n
	
	return n, err
}

func (br *BufferedReader) ReadUntilCRLF() ([]byte, error) {

	// br.buffer[pos:end] always has valid unread data
	// you first read that data
	// if crlf found, return pos:CRLFIndex

	// if not found, we need to read more data

	// read more data semantics ->
	// if buffer has space at end, do nothing
	// else if pos > 0 -> shifting
	// else grow the buffer to twice it's size and then read
	// read data from end:

	scanIndex := br.pos

	for {
		// core task: read data from buffer and advance pos
		for i, v := range br.buffer[scanIndex:br.end] {
			absoluteIndex := scanIndex + i

			if absoluteIndex > 0 && br.buffer[absoluteIndex-1] == '\r' && v == '\n' {
				result := br.buffer[br.pos : absoluteIndex-1]
				br.pos = absoluteIndex + 1
				return result, nil
			}
		}

		scanIndex = br.end

		// CRLF not found, read more data
		n, err := br.readIntoBuffer()

		if err != io.EOF && err != nil {
			return nil, err
		}

		if err == io.EOF && n == 0 {
			return nil, errors.New("CRLF not found")
		}
	}
}

// func (b *BufferedReader) ReadExact(n int) ([]byte, error) {

// }

package aof

import (
	"os"
	"server/commands"
	"server/commands/serializer"
	"sync"
	"time"
)

var MAX_BUFFER_BYTES = 512 * 1024
var FLUSH_INTERNAL_BUFFER_AFTER_T uint = 1

var AofChan = make(chan *commands.RedisCommand)

// MODEL: 2 GOROUTINES
// ONE FOR FLUSHING THE OS BUFFER
// ANOTHER FOR RECEIVING COMMANDS

func StartAof() *Aof {
	aof := initAof()
	go aof.recieveCommands()

	return aof
}


type Aof struct {
	file *os.File
	sr *serializer.Serializer

	byteCommands []byte			// serialized redis commands
	size int

	lastFlushed time.Time
	lastSynced time.Time
}

var (
	instance *Aof
	once sync.Once
)

func initAof() *Aof {
	once.Do(func() {
		file, err := os.OpenFile(
			"appendonly.aof",
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0644,
		)	

		if err != nil {
			panic(err)
		}

		instance = &Aof{
			file: file,
			sr: serializer.NewSerializer(),
			byteCommands: make([]byte, 0, MAX_BUFFER_BYTES),
			size: 0,
			lastFlushed: time.Now(),
			lastSynced: time.Now(),
		}
	})

	return instance
}

func (aof *Aof) recieveCommands() {
	writeTicker := time.NewTicker(time.Second * 1)
	syncTicker := time.NewTicker(time.Second * 1)

	defer writeTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
			case cmd := <-AofChan:
				aof.AddCommand(cmd)
			case <-writeTicker.C:
				aof.FlushBytes()
			case <-syncTicker.C:
				aof.flushOSBuffer()
		}
	}
}

func (aof *Aof) flushOSBuffer() {
	aof.file.Sync()
	aof.lastSynced = time.Now()
}

func (aof *Aof) AddCommand(cmd *commands.RedisCommand) {
	serializedCmd := aof.sr.SerializeCommand(cmd)
	size := len(serializedCmd)

	// command size itself bigger than the buffer, push immediately
	if size > MAX_BUFFER_BYTES {
		aof.FlushBytes()
		aof.WriteBytes(serializedCmd)
		return
	}

	if size + aof.size > MAX_BUFFER_BYTES {
		aof.FlushBytes()
	}

	aof.AppendCmdToBuffer(serializedCmd)
}

func (aof *Aof) WriteBytes(buf []byte) error {
	for len(buf) > 0 {
		n, err := aof.file.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

func (aof *Aof) FlushBytes() {
	if len(aof.byteCommands) == 0 {
		return
	}
	aof.WriteBytes(aof.byteCommands)
	aof.byteCommands = aof.byteCommands[:0]
	aof.size = 0
	aof.lastFlushed = time.Now()
}

func (aof *Aof) AppendCmdToBuffer(serializedCmd []byte) {
	size := len(serializedCmd)
	aof.byteCommands = append(aof.byteCommands, serializedCmd...)
	aof.size += size
}
package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// StringList is simple struct for storing a list of strings
type StringList struct {
	list []string
	mu   sync.RWMutex
}

func (l *StringList) add(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.list = append(l.list, value)
}

func (l *StringList) remove(key string) {
	var c bool
	var i int
	if c, i = l.contains(key); c {
		l.mu.Lock()
		l.list = append(l.list[:i], l.list[i+1:]...)
		defer l.mu.Unlock()
	}
}

func (l *StringList) contains(key string) (bool, int) {
	var i int
	var k string
	l.mu.RLock()
	defer l.mu.RUnlock()
	for i, k = range l.list {
		if k == key {
			return true, i
		}
	}
	return false, 0
}

func (c *Client) send(d []byte) {
	var head [4]byte
	var packet []byte
	var size = head[:0]
	var b bytes.Buffer
	var w *zlib.Writer
	size = append(size, "0000"...)

	if c.Compression {
		w = zlib.NewWriter(&b)
		w.Write(d)
		w.Close()

		binary.BigEndian.PutUint32(size, uint32(b.Len()))
		packet = append(size, b.Bytes()...)
	} else {
		binary.BigEndian.PutUint32(size, uint32(len(d)))
		packet = append(size, d...)
	}
	c.socket.Write(packet)
}

// Message represents a message that is distributed
type Message struct {
	Data    string    `msgpack:"d"`
	ID      uuid.UUID `msgpack:"id"`
	Channel string    `msgpack:"channel"`
}

// Configuration defines any variables that would be used for Grater
type Configuration struct {
	Hostname   string `json:"hostname"`
	Port       string `json:"port"`
	ConfigPath string `json:"-"`
}

// ClientManager keeps track of all clients and channels
type ClientManager struct {
	clients  map[string]Client
	channels map[string]Channel
	messages map[uuid.UUID]Message

	// Mutexes to ensure concurrency does not read and write simultaneously
	clientMutex  sync.RWMutex
	channelMutex sync.RWMutex
	messageMutex sync.RWMutex

	log zerolog.Logger

	addedMessages   *Accumulator // accumulator of messages added
	removedMessages *Accumulator // accumulator of messages removed
}

// Client manages each client and their information
type Client struct {
	ID        string
	ready     bool // represents if the client has identified
	available bool // represents if the client is waiting on any channels

	State         int      `msgpack:"state"`      // first bit set represents consumer and second represents producer
	Subscriptions []string `msgpack:"channels"`   // slice containing all channels to receive from
	Compression   bool     `msgpack:"compress"`   // if true, send data through zlib before dispatching
	BurstEnabled  bool     `msgpack:"burst"`      // boolean if the client would like burst messages
	BurstCount    int      `msgpack:"burstcount"` // number of messages burst at once

	WaitQueue StringList    // queue of messages currently waiting on. More messages will be sent once this is empty or times out
	Timeout   time.Duration // used as a timeout for wait queue

	mu            sync.RWMutex // mutex to prevent multiple channel race conditions
	socket        net.Conn     // represents the connection so data can be sent
	activeroutine bool         // boolean storing if the goroutine distributing from buffer is active
}

// Channel dictates how messages are routed
type Channel struct {
	name   string
	pool   StringList // pool represents a slice of all clients that are subscribed
	buffer *Deque     // buffer stores a deque that will supply messages in the order was sent

	bmu sync.RWMutex // mutex to prevent buffer interaction concurrently
	pmu sync.RWMutex // mutex to prevent pool interaction concurrently
}

// Packet structures how packets are marshalled and unmarshaled
type Packet struct {
	Operation int    `msgpack:"op"` // represents the op code
	Data      []byte `msgpack:"d"`  // represents the main payload that is being sent
	Type      string `msgpack:"t"`  // represents the type (used if it is dispatch)
	Channel   string `msgpack:"c"`  // represents the channel a message is for (publishing)
}

// BurstMessagePacket is a form of Packet that is specifically for messages
type BurstMessagePacket struct {
	Operation int       `msgpack:"op"` // represents the op code
	Data      []Message `msgpack:"d"`  // represents the main payload that is being sent
	Type      string    `msgpack:"t"`  // represents the type (used if it is dispatch)
}

// MessagePacket is a form of Packet that is specifically for messages
type MessagePacket struct {
	Operation int     `msgpack:"op"` // represents the op code
	Data      Message `msgpack:"d"`  // represents the main payload that is being sent
	Type      string  `msgpack:"t"`  // represents the type (used if it is dispatch)
}

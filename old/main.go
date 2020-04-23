package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack"
)

// StartupData defines the variables that will be used for configs.
type StartupData struct {
	Address string `json:"address"`
}

// ClientManager contains all information relating to the socket
type ClientManager struct {
	clients  map[string]Client
	channels map[string]Channel
	sync.Mutex
	running int
}

// MessagePacket represents the strucuture for data producers should send and is sent to consumers
type MessagePacket struct {
	Channel   string `msgpack:"c,omitempty"`
	Type      string `msgpack:"t,omitempty"`
	Data      string `msgpack:"d"`
	Operation int    `msgpack:"op"`
}

// Client wraps the connection.
type Client struct {
	socket     net.Conn // WebSocket connection.
	available  bool     // Boolean if waiting for data
	ready      bool     // Boolean if received necessary start data
	Channels   []string `msgpack:"channels"` // Channels the client is subscribed to
	ID         string   `msgpack:"id"`       // Identification for client
	IsProducer bool     `msgpack:"producer"` // Boolean if is producer
	Compress   bool     `msgpack:"compress"` // Boolean if should compress
}

// Channel contains all information that will be distributed to clients
type Channel struct {
	deque     *Deque          // Deque of channel messages
	name      string          // Name of the channel
	lock      bool            // Boolean if a task for publishing is active
	available map[string]bool // String of available consumers
	amu       sync.Mutex      // Mutex for allowing concurrency with map
}

var upgrader = websocket.Upgrader{} // Default options

func (c *Client) sendMessage(op int, data []byte, channel string) {
	packet := MessagePacket{
		Operation: op,
		Data:      string(data),
		Channel:   channel,
	}
	pack, err := msgpack.Marshal(packet)
	if err != nil {
		log.Printf("[client %s] Failed to marshal packet: %s\n", c.ID, err)
		return
	}
	if c.Compress {
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		w.Write(pack)
		w.Close()
		c.socket.Write(b.Bytes())
	} else {
		c.socket.Write([]byte(pack))
	}
}

func (cm *ClientManager) handleConnections(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
			return
		}
		go cm.handleConnection(c)
	}
}

func (cm *ClientManager) handleConnection(c net.Conn) {
	defer c.Close()

	client, err := cm.createClient(c)
	if err != nil {
		log.Printf("Failed to create client: %s\n", err)
		return
	}

	defer cm.cleanConnection(client)
	log.Printf("[client %s] Serving %s\n", client.ID, c.RemoteAddr().String())

	for {
		netData, err := bufio.NewReader(c).ReadString('\000')
		if err != nil {
			log.Printf("[client %s] Failed to read from socket: %s\n", client.ID, err)
			return
		}
		message := strings.TrimRight(string(netData), "\000")

		var packet MessagePacket

		// if client.Compress {
		// 	var b bytes.Buffer
		// 	w := zlib.NewWriter(&b)
		// 	w.Write([]byte(message))
		// 	w.Close()
		// 	err = msgpack.Unmarshal(b.Bytes(), &packet)
		// 	if err != nil {
		// 		log.Printf("[client %s] Failed to unmarshal: %s\n", client.ID, err)
		// 	}
		// } else {
		// 	err = msgpack.Unmarshal([]byte(message), &packet)
		// 	if err != nil {
		// 		log.Printf("[client %s] Failed to unmarshal: %s\n", client.ID, err)
		// 	}
		// }

		println(message)
		err = msgpack.Unmarshal([]byte(message), &packet)
		if err != nil {
			log.Printf("[client %s] Failed to unmarshal: %s\n", client.ID, err)
		}

		// log.Printf("[client %s] Received packet with op %d\n", client.ID, packet.Operation)

		// identify
		if packet.Operation == 1 {
			// IDENTIFY
			msgpack.Unmarshal([]byte(packet.Data), &client)
			client.ready = true
			cm.Lock()
			cm.clients[client.ID] = client
			cm.Unlock()

			// Remove availability in event they have reidentified
			for id := range cm.channels {
				amu := cm.channels[id].amu
				amu.Lock()
				delete(cm.channels[id].available, client.ID)
				amu.Unlock()
			}

			// Add client to channel pool
			for _, channelName := range client.Channels {
				_ = cm.getOrCreateChannel(channelName)
				amu := cm.channels[channelName].amu
				amu.Lock()
				cm.channels[channelName].available[client.ID] = false
				amu.Unlock()
			}

			cm.makeAvailable(client)
		}

		if packet.Operation == 2 && client.IsProducer {
			// PUBLISH
			log.Printf("[client %s] Publishing to %s with %s\n", client.ID, packet.Channel, packet.Data)
			cm.publishToChannel(client, packet.Channel, []byte(packet.Data))

			// if _, ok := cm.channels[packet.Channel]; ok {
			// 	log.Printf("[client %s] Publishing to %s with %s\n", packet.Channel, packet.Data)
			// 	cm.publishToChannel(client, packet.Channel, []byte(packet.Data))
			// } else {
			// 	log.Printf("[client %s] Referenced unknown channel %s\n", client.ID, packet.Channel)
			// }
		}

		if packet.Operation == 3 && !client.IsProducer {
			// ACK
			cm.makeAvailable(client)
		}
	}
}

func (cm *ClientManager) createClient(c net.Conn) (Client, error) {
	log.Printf("Creating client for %s...\n", c.RemoteAddr().String())
	client := Client{}
	client.socket = c

	uuid, err := uuid.NewUUID()
	if err != nil {
		return client, err
	}
	client.ID = uuid.String()

	log.Printf("[client %s] Created client for %s\n", client.ID, c.RemoteAddr().String())
	cm.Lock()
	cm.clients[client.ID] = client
	cm.Unlock()
	return client, nil
}

func (cm *ClientManager) makeAvailable(client Client) {
	client.available = true
	cm.Lock()
	cm.clients[client.ID] = client
	cm.Unlock()
	for _, name := range client.Channels {
		if channel, ok := cm.channels[name]; ok {
			amu := cm.channels[name].amu
			amu.Lock()
			cm.channels[name].available[client.ID] = true
			amu.Unlock()
			if !channel.lock && channel.deque.Len() > 0 {
				log.Printf("[client %s] Restarting job for channel %s", client.ID, channel.name)
				channel.lock = true
				cm.channels[name] = channel
				go cm.publishMessages(channel)
			}
		}
	}
	// log.Printf("[client %s] Made available\n", client.ID)
}

func (cm *ClientManager) cleanConnection(client Client) {
	log.Printf("[client %s] Cleaning connection\n", client.ID)
	for id := range cm.channels {
		amu := cm.channels[id].amu
		amu.Lock()
		delete(cm.channels[id].available, client.ID)
		amu.Unlock()
	}
	cm.Lock()
	delete(cm.clients, client.ID)
	cm.Unlock()
}

// publishMessages is a task function that sends its message queue to clients
func (cm *ClientManager) publishMessages(c Channel) {
	cm.running++
	var err error = nil
	cm.Lock()
	defer cm.Unlock()
	println(cm.running)

	for {
		message, err := c.deque.PopLeft()
		if message == nil {
			break
		}
		if err != nil {
			log.Printf("[clientManager] Error occured with publish job: %s", err)
		}
		participant := false

		// Iterate over clients that may be available
		amu := cm.channels[c.name].amu
		amu.Lock()
		clone := make(map[string]bool)
		for i, k := range c.available {
			clone[i] = k
		}
		amu.Unlock()

		for id, available := range clone {
			// Get the client if it is marked as available
			if available {
				cm.Lock()
				client := cm.clients[id]
				cm.Unlock()
				// Check if the client is ready and isn't processing another message
				if !client.IsProducer && client.ready && client.available {
					// Mark the mutex for the client to ensure it is not sent any more events until processed
					participant = true
					c.available[id] = false
					client.available = false
					log.Printf("Sent message to %s from channel %s\n", id, c.name)

					if s, ok := message.(string); ok {
						if client.Compress {
							var b bytes.Buffer
							w := zlib.NewWriter(&b)
							w.Write([]byte(s))
							w.Close()
							client.socket.Write(append(b.Bytes(), '\000'))
						} else {
							client.socket.Write(append([]byte(s), '\000'))
						}
					}
				}
			}
		}
		if !participant {
			// Requeue message and stop publish goroutine
			c.deque.AppendLeft(message)
			log.Printf("[channel %s] No participants available. Requeued message\n", c.name)
			break
		}
	}
	println("Unlocked channel :)")
	cm.running--
	c.lock = false
	cm.channels[c.name] = c
	if err != nil {
		log.Println(err)
		return
	}
}

func (cm *ClientManager) getOrCreateChannel(name string) Channel {
	var channel Channel
	channel, ok := cm.channels[name]
	if !ok {
		channel = Channel{
			name:      name,
			lock:      false,
			available: make(map[string]bool),
		}
		channel.deque = NewDeque()
		cm.channels[name] = channel
	}
	return channel
}

func (cm *ClientManager) publishToChannel(refer Client, name string, message []byte) {
	// log.Printf("[client %s] Triggered publish to channel %s\n", refer.ID, name)
	channel := cm.getOrCreateChannel(name)
	channel.deque.Append(string(message))

	if !channel.lock {
		channel.lock = true
		cm.channels[name] = channel
		go cm.publishMessages(channel)
	}
}

func main() {
	data := StartupData{}
	file, _ := os.Open("data.json")
	fileBytes, _ := ioutil.ReadAll(file)
	json.Unmarshal(fileBytes, &data)

	log.Println("Starting server...")
	l, err := net.Listen("tcp4", data.Address)
	if err != nil {
		log.Println(err)
		return
	}

	clientManager := ClientManager{
		clients:  make(map[string]Client),
		channels: make(map[string]Channel),
		running:  0,
	}
	go clientManager.handleConnections(l)

	log.Printf("Running on %s (CTRL + C to quit)\n", data.Address)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	<-sc

	log.Println("Shutting down...")

	for name, channel := range clientManager.channels {
		if !channel.lock {
			channel.lock = true
			clientManager.channels[name] = channel
			log.Printf("Requeueing publisher for %s", name)
			go clientManager.publishMessages(channel)
		}
	}

	start := time.Now()
	for time.Now().Sub(start) < (time.Second * 10) {
		total := 0
		for _, channel := range clientManager.channels {
			total += channel.deque.Len()
		}
		if total == 0 {
			break
		}
		log.Printf("Waiting for %d messages... %s/10s\n", total, time.Now().Sub(start))
		time.Sleep(time.Second)
	}

	for _, client := range clientManager.clients {
		client.socket.Write([]byte("EOF"))
		client.socket.Close()
	}
}

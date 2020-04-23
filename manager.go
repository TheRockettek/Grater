package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack"
)

const (
	// BurstCountDefault defines the defaut burst count on a client. This can be rewritten by client during identify
	BurstCountDefault = 5
)

// postStats simply prints incomming and outgoing message count
func (cm *ClientManager) postStats() {
	var added int64
	var removed int64
	var remain int64
	for {
		time.Sleep(time.Second)
		added = cm.addedMessages.GetLastSecond()
		removed = cm.removedMessages.GetLastSecond()
		remain = cm.addedMessages.incr - cm.removedMessages.incr
		cm.log.Info().Int64("▲", added).Int64("▼", removed).Int64("Difference", added-removed).Int64("Remain", remain).Send()
	}

}

// handleConnection accepts and incomming connections and creates their own goroutines
func (cm *ClientManager) handleConnections(l net.Listener) {
	var conn net.Conn
	var err error
	for {
		conn, err = l.Accept()
		if err != nil {
			cm.log.Error().Msgf("Raised exception whilst accepting connection: %s", err)
		}
		go cm.handleConnection(conn)
	}
}

// handleConnection creates the connection's client and reads data
func (cm *ClientManager) handleConnection(conn net.Conn) {
	defer conn.Close()
	var c Client
	var err error

	var packetSizeHeader [4]byte
	var packetData []byte
	var packetSizeBytes = packetSizeHeader[:0]
	packetSizeBytes = append(packetSizeBytes, "0000"...)
	var packetSize int

	var buffered int
	var reader *bufio.Reader
	var zlibreader io.ReadCloser
	var packet Packet

	var mids []string
	var mid string
	var wqlen int

	var ch Channel

	c, err = cm.createClient(conn)
	if err != nil {
		cm.log.Error().Msgf("Raised exception whilst creating client: %s", err)
		return
	}

	cm.log.Info().Str("c", c.ID).Str("conn", conn.RemoteAddr().String()).Msg("Serving socket")

	reader = bufio.NewReader(conn)

	for {
		// Reads 4 bytes from message to the buffer which determines size of packet
		buffered, err = io.ReadFull(reader, packetSizeBytes)
		if err == io.EOF {
			cm.log.Info().Msgf("Client %s has closed connection", c.ID)
			break
		}
		if err != nil {
			cm.log.Error().Msgf("Raised exception whilst reading from buffer %s", err)
			return
		}

		// Parse the packet size from the first 4 message bytes then create a buffer for the expected packet size
		packetSize = int(binary.BigEndian.Uint32(packetSizeBytes))
		packetData = make([]byte, packetSize)

		buffered, err = io.ReadFull(reader, packetData)
		if err == io.EOF {
			cm.log.Info().Msgf("Client %s has closed connection", c.ID)
			break
		}
		if err != nil {
			cm.log.Error().Msgf("Raised exception whilst reading from buffer %s", err)
			return
		}

		// If this ever happens were fucked.
		if buffered != packetSize {
			cm.log.Error().Bytes("packet", packetSizeBytes).Int("size", packetSize).Int("buffered", buffered).Msg("Buffer is not equal to expected message size. Panicking.")
			return
		}

		zlibreader, err = zlib.NewReader(bytes.NewReader(packetData))
		if err != nil {
			if err != zlib.ErrHeader {
				cm.log.Error().Msgf("Raised exception whilst zlib decompressing packet %s", err)
				return
			}
		} else {
			// Overwrite packetData if it can be compressed
			packetData, err = ioutil.ReadAll(zlibreader)
			zlibreader.Close()
			if err != nil {
				cm.log.Error().Msgf("Raised exception whilst reading zlib reader %s", err)
				return
			}
		}

		packet = Packet{}
		msgpack.Unmarshal(packetData, &packet)

		println(packet.Operation)
		switch packet.Operation {
		case 1:
			msgpack.Unmarshal([]byte(packet.Data), &c)
			c.ready = true
			cm.clientMutex.Lock()
			cm.clients[c.ID] = c
			cm.clientMutex.Unlock()

			if (c.State & 1) != 0 {
				c.mu.Lock()
				for _, chname := range c.Subscriptions {
					ch = cm.getChannel(chname)
					ch.pool.add(c.ID)
					cm.channelMutex.Lock()
					cm.channels[chname] = ch
					cm.channelMutex.Unlock()
				}
				c.mu.Unlock()
			}

			cm.makeAvailable(c)
		case 2:
			// Check if is producer. A producer is marked by the second bit being 1 in their state
			if (c.State & 2) != 0 {
				cm.log.Debug().Str("ch", packet.Channel).Str("c", c.ID).Int("packet", len(packet.Data)).Msgf("Publishing message")
				cm.publishToChannel(c, packet.Channel, string(packet.Data))
			}
		case 3:
			// Check if is consumer. A consumer is marked by the first bit being 1 in their state
			if (c.State & 1) != 0 {
				cm.clientMutex.RLock()
				c = cm.clients[c.ID]
				cm.clientMutex.RUnlock()

				mids = make([]string, 0)
				err = msgpack.Unmarshal(packet.Data, &mids)
				for _, mid = range mids {
					fmt.Printf("Received %s\n", mid)
					c.WaitQueue.remove(mid)
				}

				c.WaitQueue.mu.RLock()
				wqlen = len(c.WaitQueue.list)
				c.WaitQueue.mu.RUnlock()

				cm.clientMutex.Lock()
				cm.clients[c.ID] = c
				cm.clientMutex.Unlock()

				println("Queue Length", wqlen)
				if wqlen == 0 {
					cm.makeAvailable(c)
				}
			}
		}
	}
	cm.log.Trace().Msg("Finished loop")
	defer cm.removeClient(c)
}

// getChannel returns the channel from the state or creates one if it does not exist
func (cm *ClientManager) getChannel(chname string) Channel {
	var ch Channel
	var ok bool

	cm.channelMutex.RLock()
	ch, ok = cm.channels[chname]
	cm.channelMutex.RUnlock()

	if !ok {
		ch = Channel{
			name:   chname,
			pool:   StringList{},
			buffer: NewDeque(),
		}

		cm.channelMutex.Lock()
		cm.channels[ch.name] = ch
		cm.channelMutex.Unlock()
	}

	return ch
}

// createClient creates a client for a connection and adds it to the client pool
func (cm *ClientManager) createClient(conn net.Conn) (Client, error) {
	var c Client
	var id uuid.UUID
	var err error

	c = Client{
		WaitQueue:     StringList{},
		BurstEnabled:  false,
		BurstCount:    BurstCountDefault,
		activeroutine: false,
	}
	c.socket = conn

	id, err = uuid.NewUUID()
	if err != nil {
		cm.log.Error().Msgf("Raised exception whilst creating UUID: %s", err)
		return c, err
	}
	c.ID = id.String()

	cm.log.Info().Str("Addr", conn.RemoteAddr().String()).Str("ID", c.ID).Msg("Created new client")

	cm.clientMutex.Lock()
	cm.clients[c.ID] = c
	cm.clientMutex.Unlock()

	return c, nil
}

// removeClient removes the client from channel pools and the global pool. Use before closing
func (cm *ClientManager) removeClient(c Client) {
	var ok bool
	var message Message
	var ch Channel
	var _uuid uuid.UUID
	var muuid string
	var err error

	cm.log.Info().Msgf("Removing client %s", c.ID)

	// remove client from all channels that they may be in
	cm.channelMutex.Lock()
	for _, ch = range cm.channels {
		ch.pmu.Lock()
		ch.pool.remove(c.ID)
		ch.pmu.Unlock()
		cm.channels[ch.name] = ch
	}
	cm.channelMutex.Unlock()
	c.WaitQueue.mu.Lock()
	for _, muuid = range c.WaitQueue.list {
		_uuid, err = uuid.Parse(muuid)
		if err == nil {
			cm.messageMutex.RLock()
			message, ok = cm.messages[_uuid]
			cm.messageMutex.RUnlock()
			if ok {
				cm.channelMutex.RLock()
				ch, ok = cm.channels[message.Channel]
				cm.channelMutex.RUnlock()
				if ok {
					ch.bmu.Lock()
					ch.buffer.AppendLeft(_uuid.String())
					ch.bmu.Unlock()
				} else {
					cm.log.Warn().Msgf("Referenced unknown channel %s", message.Channel)
				}
			} else {
				cm.log.Warn().Msgf("Referenced unknown message %s", muuid)
			}
		} else {
			cm.log.Warn().Msgf("%s is not a valid UUID got exception %s", muuid, err)
		}
	}
	c.WaitQueue.mu.Unlock()

	// remove client from client pool
	cm.clientMutex.Lock()
	delete(cm.clients, c.ID)
	cm.clientMutex.Unlock()

	cm.log.Info().Msgf("Removed client %s", c.ID)
}

func (cm *ClientManager) makeAvailable(c Client) {
	c.available = true

	if !c.activeroutine && (c.State&1) != 0 {
		c.activeroutine = true

		cm.clientMutex.Lock()
		cm.clients[c.ID] = c
		cm.clientMutex.Unlock()
		go cm.clientRoutine(c.ID)
	} else {
		cm.clientMutex.Lock()
		cm.clients[c.ID] = c
		cm.clientMutex.Unlock()
	}
	cm.log.Debug().Msgf("%s is now available", c.ID)
}

func (cm *ClientManager) publishToChannel(c Client, channelName string, d string) {
	var ch Channel
	var cname string
	var ok bool
	var message Message
	var id uuid.UUID
	var err error

	ch = cm.getChannel(channelName)
	cm.addedMessages.Increment()

	id, err = uuid.NewUUID()
	if err != nil {
		cm.log.Error().Msgf("Raised exception whilst creating UUID: %s", err)
		return
	}

	message = Message{
		Data:    d,
		ID:      id,
		Channel: channelName,
	}
	cm.messageMutex.Lock()
	cm.messages[message.ID] = message
	cm.messageMutex.Unlock()

	ch.bmu.Lock()
	ch.buffer.Append(message.ID.String())
	ch.bmu.Unlock()

	ch.pmu.RLock()
	for _, cname = range ch.pool.list {
		cm.clientMutex.RLock()
		c, ok = cm.clients[cname]
		cm.clientMutex.RUnlock()

		if ok && c.available && !c.activeroutine && (c.State&1) != 0 {
			c.activeroutine = true
			cm.clientMutex.Lock()
			cm.clients[c.ID] = c
			cm.clientMutex.Unlock()
			go cm.clientRoutine(c.ID)
		}
	}
	ch.pmu.RUnlock()
}

func (cm *ClientManager) clientRoutine(ID string) {
	var c Client
	var ch Channel
	var ok bool
	var messages []Message
	var message Message
	var messageLen int
	var elem interface{}
	var err error
	var sid string
	var id uuid.UUID
	var p []byte
	var blen int

	cm.log.Info().Msgf("Starting routine for %s", ID)

	cm.clientMutex.RLock()
	c = cm.clients[ID]
	cm.clientMutex.RUnlock()

	for {
		if c.BurstEnabled {
			messages = make([]Message, 0)
			messageLen = c.BurstCount
		} else {
			messages = make([]Message, 0)
			messageLen = 1
		}

		// Get messages from channels and fill buffer
		cm.log.Debug().Msgf("Routine loop for %s", c.ID)
		for _, chname := range c.Subscriptions {
			cm.channelMutex.RLock()
			ch, ok = cm.channels[chname]
			cm.channelMutex.RUnlock()
			if ok {
				for {
					ch.buffer.Lock()
					blen = ch.buffer.Len()
					ch.buffer.Unlock()
					if blen > 0 && len(messages) < messageLen {
						ch.bmu.RLock()
						elem, err = ch.buffer.Pop()
						ch.bmu.RUnlock()

						sid, ok = elem.(string)
						if ok {
							id, err = uuid.Parse(sid)
							if err != nil {
								cm.log.Warn().Msg("Failed parse")
							}
							cm.messageMutex.RLock()
							message, ok = cm.messages[id]
							cm.messageMutex.RUnlock()
							if !ok {
								cm.log.Warn().Msg("Message doesnt exist")
							}
							messages = append(messages, message)
						} else {
							cm.log.Warn().Msgf("elem %s cant be turned to string", elem)
							break
						}
					} else {
						break
					}
				}
			}
		}
		if len(messages) == 0 {
			cm.log.Debug().Msgf("Exiting client routine for %s as no messages in subscriptions", c.ID)
			break
		} else {
			for len(messages) > messageLen {
				message = messages[len(messages)-1]
				messages = messages[:len(messages)-1]

				cm.channelMutex.RLock()
				ch, ok = cm.channels[message.Channel]
				cm.channelMutex.RUnlock()

				cm.log.Info().Int("Expected", messageLen).Int("BufferLen", len(messages)+1).Msgf("Rescheduled packet as surpassed buffer length")

				ch.bmu.Lock()
				ch.buffer.AppendLeft(message.ID.String())
				ch.bmu.Unlock()

				cm.channelMutex.Lock()
				cm.channels[message.Channel] = ch
				cm.channelMutex.Unlock()
			}

			if c.BurstEnabled {
				p, err = msgpack.Marshal(BurstMessagePacket{
					Operation: 0,
					Type:      "BURST",
					Data:      messages,
				})
				if err != nil {
					cm.log.Error().Msgf("Exception raised whilst marshalling packet: %s", err)
					continue
				}
				for _, message = range messages {
					c.WaitQueue.add(message.ID.String())
					cm.removedMessages.Increment()
				}
				c.send(p)
			} else {
				p, err = msgpack.Marshal(MessagePacket{
					Operation: 0,
					Type:      "MESSAGE",
					Data:      messages[0],
				})
				if err != nil {
					cm.log.Error().Msgf("Exception raised whilst marshalling packet: %s", err)
					continue
				}
				c.WaitQueue.add(messages[0].ID.String())
				cm.removedMessages.Increment()
				fmt.Printf("Sent client %s\n", messages[0].ID.String())
				c.send(p)
			}
			c.available = false
			cm.clientMutex.Lock()
			cm.clients[c.ID] = c
			cm.clientMutex.Unlock()
			cm.log.Debug().Msgf("%s is not available", c.ID)
		}
	}

	c.activeroutine = false
	cm.clientMutex.Lock()
	cm.clients[c.ID] = c
	cm.clientMutex.Unlock()
	cm.log.Info().Msgf("Exited routine for %s", c.ID)
}

// func (cm *ClientManager) channelRoutine(chname string) {
// 	var elem interface{}
// 	var err error
// 	var clientsAvailable bool
// 	var c Client
// 	var m string
// 	var p []byte
// 	var cid string
// 	var ok bool
// 	var ch Channel

// 	cm.log.Info().Msgf("Started channel routine for channel %s", ch.name)
// 	defer cm.log.Warn().Msgf("Ended channel routine for channel %s", ch.name)

// 	for {
// 		cm.channelMutex.RLock()
// 		ch = cm.channels[chname]
// 		cm.channelMutex.RUnlock()

// 		ch.bmu.Lock()
// 		elem, err = ch.buffer.Pop()
// 		ch.bmu.Unlock()

// 		if err != nil && err != ErrDequeEmpty {
// 			cm.log.Error().Msgf("Exception raised whilst popping from channel buffer: %s", err)
// 			continue
// 		}

// 		clientsAvailable = false
// 		if elem != nil {
// 			ch.pool.mu.RLock()
// 			for _, cid = range ch.pool.list {
// 				cm.clientMutex.Lock()
// 				c, ok = cm.clients[cid]
// 				cm.clientMutex.Unlock()
// 				if ok {
// 					if c.ready && c.available {
// 						c.mu.Lock()
// 						// Reduce likelyhood of race conditions where 2 channels send at same time
// 						if c.available {
// 							cm.log.Debug().Str("c", c.ID).Str("ch", ch.name).Msg("Found available channel")
// 							if m, ok = elem.(string); ok {
// 								p, err = msgpack.Marshal(Packet{
// 									Operation: 0,
// 									Type:      "MESSAGE",
// 									Data:      m,
// 									Channel:   ch.name,
// 								})
// 								if err != nil {
// 									cm.log.Error().Msgf("Exception raised whilst marshalling packet: %s", err)
// 									continue
// 								}
// 								c.send(p)

// 								cm.removedMessages.Increment()

// 								clientsAvailable = true
// 								c.available = false
// 								cm.clientMutex.Lock()
// 								cm.clients[c.ID] = c
// 								cm.clientMutex.Unlock()
// 								break
// 							} else {
// 								cm.log.Warn().Msg("Could not turn elem to string")
// 							}
// 						}
// 						c.mu.Unlock()
// 					}
// 				}
// 			}
// 			ch.pool.mu.RUnlock()
// 		} else {
// 			cm.log.Debug().Msgf("Channel %s buffer has been emptied, waiting", ch.name)
// 		}

// 		// If no clients are available, reque message and finish loop
// 		if !clientsAvailable {
// 			if err == ErrDequeEmpty {
// 				cm.log.Debug().Msgf("Readded packet back to queue as no clients were available")

// 				println("Waiting for writers")
// 				ch.chy = true
// 				cm.channelMutex.RLock()
// 				cm.channels[chname] = ch
// 				cm.channelMutex.RUnlock()
// 				<-ch.wch
// 			} else {
// 				cm.log.Debug().Msgf("Readded packet back to queue as no clients were available")
// 				ch.bmu.Lock()
// 				ch.buffer.AppendLeft(elem)
// 				ch.bmu.Unlock()

// 				println("Waiting for readers")
// 				ch.chy = false
// 				cm.channelMutex.RLock()
// 				cm.channels[chname] = ch
// 				cm.channelMutex.RUnlock()
// 				<-ch.rch
// 			}
// 			println("Received")
// 		}
// 	}
// 	// ch.activeroutine = false
// 	// cm.channelMutex.Lock()
// 	// cm.channels[ch.name] = ch
// 	// cm.channelMutex.Unlock()
// }

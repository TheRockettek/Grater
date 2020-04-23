package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func main() {
	var err error
	var file []byte
	var clientManager ClientManager
	var listener net.Listener
	var sc chan os.Signal
	var ch Channel
	var start time.Time
	var total int
	var c Client
	var configuration *Configuration

	configuration = new(Configuration)
	flag.StringVar(&configuration.Hostname, "hostname", "127.0.0.1", "Hostname that sockets will connect to. Set to 0.0.0.0 to broadcast.")
	flag.StringVar(&configuration.Port, "port", "50000", "Port that sockets will connect to.")
	flag.StringVar(&configuration.ConfigPath, "configpath", "", "Path of json config file")
	flag.Parse()

	if configuration.ConfigPath != "" {
		if _, err = os.Stat(configuration.ConfigPath); err == nil {
			file, _ = ioutil.ReadFile(configuration.ConfigPath)
			json.Unmarshal(file, &configuration)
		}
	}

	clientManager = ClientManager{
		clients:         make(map[string]Client),
		channels:        make(map[string]Channel),
		messages:        make(map[uuid.UUID]Message),
		addedMessages:   NewAccumulator(),
		removedMessages: NewAccumulator(),
		log: zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.Stamp,
		}).With().Timestamp().Logger(),
	}

	clientManager.log.Info().Msg("Started client manager")

	listener, err = net.Listen("tcp", configuration.Hostname+":"+configuration.Port)
	if err != nil {
		clientManager.log.Fatal().Msg(err.Error())
	}

	go clientManager.handleConnections(listener)
	go clientManager.postStats()

	// zerolog.SetGlobalLevel(zerolog.InfoLevel)
	clientManager.log.Info().Str("hostname", configuration.Hostname).Str("port", configuration.Port).Msg("Running server (CTRL + C to quit)")

	sc = make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	<-sc

	start = time.Now()
	for time.Now().Sub(start) < (time.Second * 10) {
		total = 0
		clientManager.channelMutex.Lock()
		for _, ch = range clientManager.channels {
			ch.buffer.Lock()
			total += ch.buffer.Len()
			ch.buffer.Unlock()
		}
		clientManager.channelMutex.Unlock()
		if total == 0 {
			break
		}
		log.Printf("Waiting for %d messages... %s/10s\n", total, time.Now().Sub(start))
		time.Sleep(time.Second)
	}

	clientManager.clientMutex.Lock()
	for _, c = range clientManager.clients {
		c.socket.Close()
	}
	clientManager.clientMutex.Unlock()
}

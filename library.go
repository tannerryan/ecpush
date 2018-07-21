// Copyright (c) 2018, Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD 2-clause
// license that can be found in the LICENSE file.

/*
Package ecpush is a library for subscribing to real-time
meteorological data feeds from Environment Canada.

Goals

The main goal of ecpush is to provide a simple and lightweight
client that can be used for receiving real-time data events
directly from Environment Canada's meteorological product
feed.

The client does not directly fetch the published products,
but provides a notification channel containing the product
location (HTTP URL to Environment Canada's Datamart).

The client has also been designed to fully and properly recover
from disconnections, without the need to prompt a reconnection.

Usage

The interface is very minimal. To create a new client, simply
create a Client struct. The only required field in the struct
is the Subtopics array. Default values for the other fields
are listed in the struct definition. An example configuration
is shown below (subscribing to text bulletins and citypage XML).

Please see https://github.com/MetPX/sarracenia/blob/master/doc/sr_subscribe.1.rst#subtopic-amqp-pattern-subtopic-need-to-be-set
for formatting subtopics.

	client := ecpush.Client{
		Subtopics: []string{"bulletins.alphanumeric.#", "citypage_weather.xml.#"},
	}

When calling Connect() on the newly created client, two channels
will be returned. A conditional on the done channel should be performed.
A nil done channel after Connection will occur if the client cannot
connect to the messaging broker and fault recovery is disabled.

To receive the Events, create a goroutine to range over the Event
channel. The done channel may be used to block the goroutine.

	if msg, done := client.Connect(); done != nil {
		go func() {
			for event := range msg {
				log.Printf("%s; %s\n", event.URL, event.Md5)
			}
		}()
		<-done
	}

To close the ecpush Client, simply call Close() on the client. This will
close all active channels and connections to the messaging broker. It
will also signal the done channel which will close the holding goroutine
previously created above.

	client.Close()

Examples

A fully functional client can be found in the example directory.

Acknowledgements

I would like to thank Sean Treadway for his Go RabbitMQ client library.
I would also like to thank Environment Canada and the awesome people at
Shared Services Canada for their developments and "openness" of MetPX
and sarracenia.

License

Copyright (c) 2018, Tanner Ryan. All rights reserved. Use of this source
code is governed by a BSD 2-clause license that can be found in the LICENSE
file.

Sean Treadway's Go RabbitMQ client library is also under a BSD 2-clause
license. Once again, all rights reserved.

*/
package ecpush

import (
	"crypto/rand"
	"fmt"
	"log"
	"strings"
	"time"

	// Copyright (c) 2012, Sean Treadway, SoundCloud Ltd. All rights reserved.
	"github.com/streadway/amqp"
)

const (
	broker       = "dd.weather.gc.ca" // domain of AMQP broker (default: dd.weather.gc.ca)
	port         = "5672"             // port of AMQP broker (default: 5672)
	user         = "anonymous"        // AMQP username (default: anonymous)
	pass         = "anonymous"        // AMQP password (default: anonymous)
	prefix       = "v02.post."        // AMQP routing key prefix (default: v02.post)
	heartbeat    = 300 * time.Second  // AMQP heartbeat interval (default: 300 seconds)
	connDelay    = 30 * time.Second   // Default reconnection delay (default: 30 seconds)
	recoverDelay = 2 * time.Second    // Process recovery delay (default: 2 seconds)
)

// Client contains the entire amqp configuration.
type Client struct {
	Subtopics       []string         // array of subscribed subtopics (see above for formatting)
	DisableRecovery bool             // disable connection and fault recovery (optional; default: false)
	DisableEventLog bool             // disable event log (optional; default: false)
	ReconnectDelay  time.Duration    // amqp reconnect delay (default; default: 30 seconds)
	conn            *amqp.Connection // streadway amqp.Connection
	ch              *amqp.Channel    // streadway amqp.Channel
	q               amqp.Queue       // streadway amqp.Queue
	out             chan Event       // client Event output channel
	done            chan bool        // client done signaling channel
}

// An Event contains a subscribed product announcement.
type Event struct {
	URL string // url of the product (located on Datamart)
	Md5 string // md5 hash of product (used for validating file downloads)
}

// Connect will establish all connection and channels required
// for receiving products. It will return an Event channel containing
// events for all subscribed subtopics. It will also return a bool
// channel for signaling client closure.
func (client *Client) Connect() (<-chan Event, chan bool) {

	client.prime()
	client.connect()

	return client.out, client.done
}

// Close will close all channels and connections to the amqp broker.
// It will also push a message on the bool channel to signal closure.
func (client *Client) Close() {
	// close amqp channel and TCP connection
	if client.ch != nil {
		client.ch.Close()
	}
	if client.conn != nil {
		client.conn.Close()
	}

	// signal goroutine that messages are done
	client.done <- true
}

// prime generates the client output and done channels for
// Event streaming. It also performs any necessary client
// bootstraping.
func (client *Client) prime() {
	// make channel for output Event and signaling completion
	out, done := make(chan Event), make(chan bool, 1)
	client.out, client.done = out, done

	// client bootstraping
	if client.ReconnectDelay == 0*time.Second {
		client.ReconnectDelay = connDelay
	}
	if len(client.Subtopics) == 0 {
		client.log("No subtopics were defined; exiting")
		client.Close()
	}
}

// connect is responsible for establishing a connection
// and a channel with the amqp messaging broker. It will
// also call the consume function when ready.
func (client *Client) connect() {
	// connect to amqp broker
	conn, err := amqp.DialConfig("amqp://"+user+":"+pass+"@"+broker+":"+port+"/", amqp.Config{
		Heartbeat: heartbeat,
	})
	if err != nil {
		client.error("Failed to connect to " + broker)
		return
	}

	client.conn = conn
	client.log("Connected to " + broker)

	// declare amqp channel
	ch, err := conn.Channel()
	if err != nil {
		client.error("Failed to declare channel")
		return
	}

	client.ch = ch
	client.log("Established channel")

	// monitor any channel and connection closures
	go func() {
		disconnect := make(chan *amqp.Error)
		client.ch.NotifyClose(disconnect)

		// encountered error
		<-disconnect
		client.error("Disconnected from " + broker)
		return
	}()

	// log about recovery settings (recovery should be enabled)
	if client.DisableRecovery {
		client.log("Disconnect and error recovery is disabled")
	} else {
		client.log("Disconnect and error recovery is enabled")
	}

	// generate queue name (random)
	qPrefix, qIdentifer, qMode := "q_"+user, "ecpush", "wx"
	q1, q2 := make([]byte, 4), make([]byte, 4)
	rand.Read(q1)
	rand.Read(q2)
	qID := qPrefix + "." + qIdentifer + "." + qMode + "." + fmt.Sprintf("%x", q1) + "." + fmt.Sprintf("%x", q2)

	// declare amqp queue
	q, err := ch.QueueDeclare(
		qID,   // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no wait
		nil,   // arguments
	)
	if err != nil {
		client.error("Failed to declare queue")
		return
	}

	client.q = q
	client.log("Declared message queue " + qID)

	// bind to provided subtopics
	for _, subtopic := range client.Subtopics {
		err = ch.QueueBind(
			q.Name,          // queue name
			prefix+subtopic, // routing key
			"xpublic",       // exchange
			false,           // no wait
			nil,             // arguments
		)
		if err != nil {
			client.error("Failed to bind " + prefix + subtopic)
			return
		}

		client.log("Listening for messages on " + prefix + subtopic)
	}

	client.log("Client successfully connected; consumer ready to activate")
	client.consume()
}

// consume establishes a new channel consumer, generates
// new events and publishes them on the client's out channel.
func (client *Client) consume() {
	// create channel consumer
	messages, err := client.ch.Consume(
		client.q.Name, // queue
		"",            // consumer
		false,         // auto ack
		false,         // exclusive
		false,         // no local
		false,         // no wait
		nil,           // arguments
	)
	if err != nil {
		client.error("Failed to consume messages")
		return
	}

	go func() {
		// deferred routine recovery for malformed messages
		defer func() {
			if r := recover(); r != nil && !client.DisableRecovery {
				client.log("Encountered malformed message; now recovering")
				time.Sleep(recoverDelay)
				client.consume()
				return
			}
		}()

		// range over channel to fetch amqp events
		for d := range messages {
			uri := strings.Split(string(d.Body), " ")
			sum := d.Headers["sum"].(string)[2:]

			// publish Event to client output channel and ack amqp message
			client.out <- Event{
				URL: string(uri[1] + uri[2]),
				Md5: sum,
			}
			d.Ack(false)
		}
	}()

	client.log("Consumer activated; messages now streaming to channel")
}

// log is a helper function to print debugging events if event
// logging is not disabled.
func (client *Client) log(data interface{}) {
	if !client.DisableEventLog {
		log.Println(data)
	}
}

// error is a helper function to resolve any connection or
// channel related issues.
func (client *Client) error(err string) {
	client.log(err)
	if !client.DisableRecovery {
		if client.conn != nil {
			client.conn.Close()
		}

		client.log("Waiting for delay to attempt reconnection")
		time.Sleep(connDelay)

		client.connect()
	} else {
		client.log("Recovery is disabled; nothing to do")
		client.Close()
	}
}

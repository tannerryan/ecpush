// Copyright (c) 2018, Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD 2-clause
// license that can be found in the LICENSE file.

package ecpush

import (
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	// Copyright (c) 2012, Sean Treadway, SoundCloud Ltd. All rights reserved.
	"github.com/streadway/amqp"
)

const (
	broker          = "dd.weather.gc.ca" // domain of AMQP broker (default: dd.weather.gc.ca)
	port            = "5672"             // port of AMQP broker (default: 5672)
	user            = "anonymous"        // AMQP username (default: anonymous)
	pass            = "anonymous"        // AMQP password (default: anonymous)
	prefix          = "v02.post."        // AMQP routing key prefix (default: v02.post)
	heartbeat       = 60 * time.Second   // AMQP heartbeat interval (default: 60 seconds)
	qosPrefetch     = 10                 // AMQP qos prefetch count (default: 10)
	connDelay       = 10 * time.Second   // default reconnection delay (default: 10 seconds)
	recoverDelay    = 1 * time.Second    // malformed message recovery delay (default: 1 seconds)
	contentAttempts = 3                  // number of HTTP content fetch attempts (default: 3)
	httpTimeout     = 60 * time.Second   // http fetch timeout (default: 60 seconds)
)

// Client contains the entire amqp configuration.
type Client struct {
	Subtopics           []string         // array of subscribed subtopics (see above for formatting)
	DisableRecovery     bool             // disable connection and fault recovery (optional; default: false)
	DisableEventLog     bool             // disable event log (optional; default: false)
	ReconnectDelay      time.Duration    // amqp reconnect delay (default; default: 10 seconds)
	NotifyOnly          bool             // disable HTTP content fetching (optional; default: false)
	DisableContentRetry bool             // disable multiple HTTP fetches in event of request failure (optional; default: false)
	ContentAttempts     int              // number of HTTP fetch attempts (optional: default 3)
	conn                *amqp.Connection // streadway amqp.Connection
	ch                  *amqp.Channel    // streadway amqp.Channel
	q                   amqp.Queue       // streadway amqp.Queue
	out                 chan *Event      // client Event output channel
	done                chan bool        // client done signaling channel
	httpClient          *http.Client     // net http client for sending HTTP requests
}

// An Event contains a subscribed product announcement.
type Event struct {
	URL            string // url of the product (located on Datamart)
	Md5            string // md5 hash of product (used for validating file downloads)
	Route          string // AMQP routing key of event
	Content        string // event contents (if NotifyOnly is false)
	ContentFailure bool   // indicator if event fetching failed
}

// Connect will establish all connection and channels required
// for receiving products. It will return an Event channel containing
// events for all subscribed subtopics. It will also return a bool
// channel for signaling client closure.
func (client *Client) Connect() (<-chan *Event, chan bool) {
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
	out, done := make(chan *Event, qosPrefetch), make(chan bool, 1)
	client.out, client.done = out, done

	// http client
	httpClient := &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	client.httpClient = httpClient

	// client bootstraping
	if client.ReconnectDelay == 0*time.Second {
		client.ReconnectDelay = connDelay
	}
	if len(client.Subtopics) == 0 {
		client.log("No subtopics were defined; exiting")
		client.Close()
	}
	if client.ContentAttempts == 0 {
		client.ContentAttempts = contentAttempts
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
	ch, err := client.conn.Channel()
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
	q, err := client.ch.QueueDeclare(
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

	// declare channel quality of service
	err = client.ch.Qos(
		qosPrefetch, // prefetch count
		0,           // prefetch size
		false,       // global qos
	)
	if err != nil {
		client.error("Failed to set channel QoS")
		return
	}
	client.log("Channel QoS successfully configured")

	// bind to provided subtopics
	for _, subtopic := range client.Subtopics {
		err = client.ch.QueueBind(
			client.q.Name,   // queue name
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

			// basic event structure
			event := &Event{
				URL:            string(uri[1] + uri[2]),
				Md5:            sum,
				Route:          d.RoutingKey,
				Content:        "",
				ContentFailure: false,
			}
			// send event to output channel as is or attempt to fetch content
			if client.NotifyOnly {
				client.out <- event
			} else {
				done := make(chan bool, 1)
				go client.fetchContent(event, done)
				<-done
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

// fetchContent will attempt to fetch the contents of an event and
// update the contents of the Event. It will also trigger the done
// channel when completed.
func (client *Client) fetchContent(event *Event, done chan bool) {
	content, err := fetchEvent(client, event, !client.DisableContentRetry)
	if err != nil {
		if client.DisableContentRetry {
			client.log("Failed to fetch event content " + event.URL + "; no retries were attempted")
		} else {
			client.log("Failed to fetch event content " + event.URL + "; max number of attempts were performed")
		}
		event.ContentFailure = true
	} else {
		event.Content = string((*content)[:])
	}

	// send modified event to output channel and trigger done
	client.out <- event
	done <- true
	return
}

// fetchEvent will fetch the HTTP contents of an Event and return
// a reference to the byte array or an error.
func fetchEvent(client *Client, event *Event, multipleAttempts bool) (*[]byte, error) {
	// wrapper for performing multiple attempts
	if multipleAttempts {
		remainingAttempts := client.ContentAttempts
		for remainingAttempts > 0 {
			content, err := fetchEvent(client, event, false)
			if err != nil {
				remainingAttempts--
			} else {
				return content, nil
			}
		}
		return nil, errors.New("failed to fetch event content")
	}

	// fetch event through standard http
	resp, err := client.httpClient.Get(event.URL)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	// read response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// validate checksum
	bodyHash := fmt.Sprintf("%x", md5.Sum(body))
	if string(bodyHash[:]) != event.Md5 {
		return nil, errors.New("invalid download; event hash does not match")
	}

	return &body, nil
}

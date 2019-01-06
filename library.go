// BSD 2-Clause License
//
// Copyright (c) 2019 Tanner Ryan. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package ecpush

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/streadway/amqp" // Copyright (c) 2012 Sean Treadway, SoundCloud Ltd. All rights reserved.
)

const (
	broker          = "dd.weather.gc.ca" // AMQP broker
	port            = "5671"             // AMQP port
	user            = "anonymous"        // AMQP username
	pass            = "anonymous"        // AMQP password
	prefix          = "v02.post."        // AMQP routing key prefix
	exchange        = "xpublic"          // AMQP exchange
	qos             = 30                 // AMQP qos prefetch
	recoverDelay    = 1 * time.Second    // reconnection + malformed message recovery delay
	contentAttempts = 3                  // number of HTTP content fetch attempts
	httpTimeout     = 15 * time.Second   // http fetch timeout
)

var (
	httpClient = &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	errGeneral = errors.New("")
)

// Client contains the entire amqp configuration.
type Client struct {
	Subtopics       *[]string        // array of subscribed subtopics (see documentation for formatting)
	DisableEventLog bool             // disable event log (default: false)
	FetchContent    bool             // enable HTTP content fetching (default: false)
	conn            *amqp.Connection // streadway amqp.Connection
	ch              *amqp.Channel    // streadway amqp.Channel
	out             chan *Event      // client Event output channel
	done            chan bool        // client done signaling channel
}

// Event contains a subscribed product announcement.
type Event struct {
	URL            string // url of the product (located on Datamart)
	Md5            string // md5 hash of product (used for content validation)
	Route          string // AMQP routing key of event
	Content        string // event contents (if FetchContent is true)
	ContentFailure bool   // indicator if event fetching failed
}

// Connect will establish all connection and channels required for receiving
// products. It will return an Event channel containing events for all
// subscribed subtopics. It will also return a bool channel for signaling client
// closure.
func (client *Client) Connect() (<-chan *Event, chan bool) {
	client.prime()
	client.connect()

	return client.out, client.done
}

// Close will close all channels and connections to the amqp broker. It will
// also push a message on the bool channel to signal closure.
func (client *Client) Close() {
	if client.ch != nil {
		client.ch.Cancel("ecpush", true)
		client.ch.Close()
	}
	if client.conn != nil {
		client.conn.Close()
	}

	client.done <- true
}

// prime generates the client output and done channels for Event streaming.
func (client *Client) prime() {
	client.out, client.done = make(chan *Event), make(chan bool, 1)

	if len(*client.Subtopics) == 0 {
		client.log("No subtopics were defined; exiting")
		client.Close()
	}
}

// connect is responsible for establishing a connection and a channel with the
// amqp messaging broker. It will also call the consume function when ready.
func (client *Client) connect() {
	var err error

	client.conn, err = amqp.DialTLS("amqps://"+user+":"+pass+"@"+broker+":"+port+"/", &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		client.error("Failed to connect to " + broker)
		return
	}
	client.log("Connected to " + broker)

	go func() {
		<-client.conn.NotifyClose(make(chan *amqp.Error))
		client.error("Disconnected from " + broker)
		return
	}()

	client.ch, err = client.conn.Channel()
	if err != nil {
		client.error("Failed to declare channel")
		return
	}
	client.log("Established channel")

	qPrefix, qIdentifer, qMode := "q_"+user, "ecpush", "wx"
	q1, q2 := make([]byte, 4), make([]byte, 4)
	rand.Read(q1)
	rand.Read(q2)
	qID := qPrefix + "." + qIdentifer + "." + qMode + "." + fmt.Sprintf("%x", q1) + "." + fmt.Sprintf("%x", q2)

	q, err := client.ch.QueueDeclare(
		qID,   // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no wait
		nil,   // arguments
	)
	if err != nil {
		client.error("Failed to declare queue")
		return
	}
	client.log("Declared message queue " + qID)

	if err = client.ch.Qos(
		qos,   // prefetch count
		0,     // prefetch size
		false, // global qos
	); err != nil {
		client.error("Failed to set channel QoS")
		return
	}
	client.log("Channel QoS successfully configured")

	for _, subtopic := range *client.Subtopics {
		if err = client.ch.QueueBind(
			q.Name,          // queue name
			prefix+subtopic, // routing key
			exchange,        // exchange
			false,           // no wait
			nil,             // arguments
		); err != nil {
			client.error("Failed to bind " + prefix + subtopic)
			return
		}
		client.log("Listening for messages on " + prefix + subtopic)
	}

	client.log("Client successfully connected; consumer ready to activate")
	client.consume(q.Name)
}

// consume establishes a new channel consumer, generates new events and
// publishes them on the client's out channel.
func (client *Client) consume(qName string) {
	messages, err := client.ch.Consume(
		qName,    // queue
		"ecpush", // consumer
		false,    // auto ack
		false,    // exclusive
		false,    // no local
		false,    // no wait
		nil,      // arguments
	)
	if err != nil {
		client.error("Failed to consume messages")
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				client.log("Encountered malformed message; now recovering")
				time.Sleep(recoverDelay)
				client.consume(qName)
				return
			}
		}()

		for d := range messages {
			uri := strings.Split(string(d.Body), " ")
			sum := d.Headers["sum"].(string)[2:]
			event := &Event{
				URL:            string(uri[1] + uri[2]),
				Md5:            sum,
				Route:          d.RoutingKey,
				Content:        "",
				ContentFailure: false,
			}

			if !client.FetchContent {
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

// log is a helper function to print debugging events if event logging is not
// disabled.
func (client *Client) log(data interface{}) {
	if !client.DisableEventLog {
		log.Println(data)
	}
}

// error is a helper function to resolve any connection or channel related
// issues.
func (client *Client) error(err string) {
	client.log(err)
	if client.conn != nil {
		client.conn.Close()
	}
	client.log("Waiting for delay to attempt reconnection")
	time.Sleep(recoverDelay)
	client.connect()
}

// fetchContent will attempt to fetch the contents of an event and update the
// contents of the Event. It will also trigger the done channel when completed.
func (client *Client) fetchContent(event *Event, done chan bool) {
	content, err := fetchEvent(event, true)
	if err != nil {
		client.log("Failed to fetch event content")
		event.ContentFailure = true
	} else {
		event.Content = string((*content)[:])
	}

	client.out <- event
	done <- true
	return
}

// fetchEvent will fetch the HTTP contents of an Event and return a reference to
// the byte array or an error.
func fetchEvent(event *Event, multipleAttempts bool) (*[]byte, error) {
	if multipleAttempts {
		remainingAttempts := contentAttempts
		for remainingAttempts > 0 {
			content, err := fetchEvent(event, false)
			if err != nil {
				remainingAttempts--
			} else {
				return content, nil
			}
		}
		return nil, errGeneral
	}

	resp, err := httpClient.Get(event.URL)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	bodyHash := fmt.Sprintf("%x", md5.Sum(body))
	if string(bodyHash[:]) != event.Md5 {
		return nil, errGeneral
	}

	return &body, nil
}

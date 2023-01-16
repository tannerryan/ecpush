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
	"context"
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

	"github.com/cenkalti/backoff/v4" // Copyright (c) 2014 Cenk Alti. All rights reserved.
	"github.com/streadway/amqp"      // Copyright (c) 2012 Sean Treadway, SoundCloud Ltd. All rights reserved.
)

const (
	broker          = "dd.weather.gc.ca" // AMQP broker
	brokerCert      = "weather.gc.ca"    // AMQP TLS hostname
	port            = 5671               // AMQP port
	user            = "anonymous"        // AMQP username
	pass            = "anonymous"        // AMQP password
	prefix          = "v02.post."        // AMQP routing key prefix
	exchange        = "xpublic"          // AMQP exchange
	qos             = 30                 // AMQP qos prefetch
	queueExpiry     = 5 * time.Minute    // AMQP remote queue expiry (after disconnect)
	maxRecoverDelay = 16 * time.Second   // reconnection + malformed message max recovery delay
	contentAttempts = 3                  // number of HTTP content fetch attempts
	httpTimeout     = 15 * time.Second   // http fetch timeout
)

var (
	// httpClient for fetching content
	httpClient = &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	// tlsConfig for AMQP connection
	tlsConfig = &tls.Config{
		ServerName: brokerCert,
	}

	// errNoSubtopics returned when no subtopics are provided
	errNoSubtopics = errors.New("ecpush: must provide at least 1 subtopic before Connect()")
	// errBadHash returned when hash of HTTP content does not match provided MD5
	errBadHash = errors.New("ecpush: provided hash does not match received content")
	// errFailedFetch returned when client fails to fetch HTTP content
	errFailedFetch = errors.New("ecpush: failed to fetch content")
)

// Client contains the ecpush consumer client.
type Client struct {
	Subtopics       *[]string                   // Subtopics are array of subscribed subtopics (see documentation for formatting)
	DisableEventLog bool                        // DisableEventLog disables the event log (default: false)
	FetchContent    bool                        // FetchContent enables HTTP content fetching (default: false)
	ctx             context.Context             // ctx is the parent context for cancellation
	event           chan *Event                 // event is client Event channel for consume function
	uid             string                      // uid is the unique client identifier
	activated       bool                        // activated indicates if consumer is activated
	conn            *amqp.Connection            // conn is streadway amqp.Connection
	ch              *amqp.Channel               // ch is streadway amqp.Channel
	delay           *backoff.ExponentialBackOff // delay is exponential backoff for connection recovery
}

// Event is a received payload from Environment Canada's datamart.
type Event struct {
	URL            string // URL of the product (located on Datamart)
	MD5            string // MD5 hash of product (used for content validation)
	Route          string // Route is AMQP routing key of event
	Content        string // Content is event contents (if FetchContent is true)
	ContentFailure bool   // ContentFailure indicates if event fetching failed
}

// Connect establishes the AMQP channel for receiving products located on
// provided subtopics. It blocks until the initial connection is established
// with the remote server. Context is passed for closing the client. Connect
// returns an error if no subtopics are provided.
func (c *Client) Connect(ctx context.Context) error {
	c.ctx = ctx
	c.event = make(chan *Event)

	// generate unique client identifier
	q1, q2 := make([]byte, 5), make([]byte, 5)
	rand.Read(q1)
	rand.Read(q2)
	c.uid = fmt.Sprintf("q_%s.ecpush.wx.%x.%x", user, q1, q2)

	// exponential backoff for connection recovery (no max)
	c.delay = backoff.NewExponentialBackOff()
	c.delay.MaxInterval = maxRecoverDelay
	c.delay.MaxElapsedTime = 0
	c.delay.Reset()

	// ensure at least one subtopic is provided
	if len(*c.Subtopics) == 0 {
		return errNoSubtopics
	}

	c.provision()
	return nil
}

// Consume returns the next event and an indicator if the client is not actively
// consuming from the remote server.
func (c *Client) Consume() (*Event, bool) {
	if !c.activated {
		// client not connected
		return nil, true
	}
	select {
	case <-c.ctx.Done():
		// client closed
		return nil, true
	case e := <-c.event:
		// next event
		return e, false
	}
}

// close will terminate the provisioned AMQP channel and connection to the
// remote server. It closes the main event channel.
func (c *Client) close() {
	c.log("[ecpush] received context cancellation, terminating consumer")

	// cancel and close channel
	if c.ch != nil {
		c.ch.Cancel(c.uid, true)
		c.ch.Close()
	}
	// close connection
	if c.conn != nil {
		c.conn.Close()
	}

	// no longer activated, close event channel
	c.activated = false
	close(c.event)
}

// provision establishes the AMQP connection and channel with the remote broker.
// On success, the necessary AMQP queues, QoS settings, and bindings are
// configured, and internal feed consumption will begin. If provisioning fails,
// the recovery function is called.
func (c *Client) provision() {
	// establish connection with remote server
	uri := fmt.Sprintf("amqps://%s:%s@%s:%d/", user, pass, broker, port)
	conn, err := amqp.DialTLS(uri, tlsConfig)
	if err != nil {
		c.recover("[ecpush] failed to connect to " + broker)
		return
	}
	c.conn = conn
	c.log("[ecpush] connected to " + broker)

	go func() {
		// begin listening for connection errors, recover on error
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.recover("[ecpush] disconnected from " + broker)
	}()

	// establish consumption channel
	ch, err := c.conn.Channel()
	if err != nil {
		c.recover("[ecpush] failed to declare channel")
		return
	}
	c.ch = ch
	c.log("[ecpush] declared channel")

	// declare queue
	q, err := c.ch.QueueDeclare(
		c.uid, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no wait
		amqp.Table{
			"x-expires": int(queueExpiry.Milliseconds()), // RabbitMQ
		}, // arguments
	)
	if err != nil {
		c.recover("[ecpush] failed to declare queue")
		return
	}
	c.log("[ecpush] declared queue " + c.uid)

	// set channel quality of service
	if err = c.ch.Qos(
		qos,   // prefetch count
		0,     // prefetch size
		false, // global qos
	); err != nil {
		c.recover("[ecpush] failed to configure channel QoS")
		return
	}
	c.log("[ecpush] configured channel QoS")

	// subscribe to all subtopics
	for _, subtopic := range *c.Subtopics {
		if err = c.ch.QueueBind(
			q.Name,          // queue name
			prefix+subtopic, // routing key
			exchange,        // exchange
			false,           // no wait
			nil,             // arguments
		); err != nil {
			c.recover("[ecpush] failed to bind queue " + prefix + subtopic)
			return
		}
		c.log("[ecpush] listening for messages on " + prefix + subtopic)
	}

	c.log("[ecpush] client provisioned, activating consumer")
	c.consume(q.Name)
}

// consume establishes an internal channel consumer. A goroutine is created for
// receiving events and emitting on event channel. The goroutine is terminated
// upon context done signal.
func (c *Client) consume(qName string) {
	// consume from queue
	messages, err := c.ch.Consume(
		c.uid, // queue
		c.uid, // consumer
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // arguments
	)
	if err != nil {
		c.recover("[ecpush] failed to consume messages from queue")
		return
	}

	go func() {
		// if messages cannot be parsed successfully, sleep and recover
		defer func() {
			if r := recover(); r != nil {
				c.log("[ecpush] received malformed message")
				c.consume(qName)
				return
			}
		}()

		// receive raw messages (event loop)
		for d := range messages {
			select {
			case <-c.ctx.Done():
				// exit on end signal
				return
			default:
				// parse raw payload and generate event
				uri := strings.Split(string(d.Body), " ")

				// correct bad paths from remote
				if !strings.HasPrefix(uri[2], "/") {
					uri[2] = "/" + uri[2]
				}

				sum := d.Headers["sum"].(string)[2:]
				event := &Event{
					URL:            string(uri[1] + uri[2]),
					MD5:            sum,
					Route:          d.RoutingKey,
					Content:        "",
					ContentFailure: false,
				}

				// fetch content and update event if required
				if c.FetchContent {
					c.fetchContent(event)
				}

				// emit and acknowledge event
				c.event <- event
				d.Ack(false)
			}
		}
	}()

	c.activated = true
	c.delay.Reset()
	c.log("[ecpush] consumer activated, ready for consumption")

	// close the client on exit signal
	go func(c *Client) {
		<-c.ctx.Done()
		c.close()
	}(c)
}

// log internally logs events if enabled.
func (c *Client) log(data interface{}) {
	if !c.DisableEventLog {
		log.Println(data)
	}
}

// recover restarts the provisioning process after an exponential backoff delay.
// It should be called on any error preventing data consumption.
func (c *Client) recover(err string) {
	select {
	case <-c.ctx.Done():
		// do not recover on cancellation
		return
	default:
	}
	// log provided error
	c.log(err)
	// close channel and connections if defined
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	// sleep and re-provision
	delay := c.delay.NextBackOff()
	c.log("[ecpush] waiting " + delay.String() + " for reconnect")
	time.Sleep(delay)
	c.provision()
}

// fetchContent accepts an Event and attempts to populate the Event with content
// located at Event URL. If the content can not be fetched after multiple
// attempts, the ContentFailure flag is set to true in the Event.
func (c *Client) fetchContent(event *Event) {
	// attempt to fetch content with multiple attempts
	content, err := fetchEvent(event, true)
	if err != nil {
		c.log("[ecpush] failed to fetch event content")
		event.ContentFailure = true
	} else {
		// populate event content
		event.Content = string((*content)[:])
	}
}

// fetchEvent recursively attempts to fetch the HTTP contents of an Event. It
// returns the byte content of the event or an error if all content fetch
// attempts return an error.
func fetchEvent(event *Event, multipleAttempts bool) (*[]byte, error) {
	if multipleAttempts {
		// recursively fetch content until success or no attempts remaining
		remainingAttempts := contentAttempts
		for remainingAttempts > 0 {
			content, err := fetchEvent(event, false)
			if err != nil {
				remainingAttempts--
			} else {
				return content, nil
			}
		}
		// ran out of attempts
		return nil, errFailedFetch
	}
	// fetch the event contents at URL
	resp, err := httpClient.Get(event.URL)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	// read body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// check the event hash to hash of body
	bodyHash := fmt.Sprintf("%x", md5.Sum(body))
	if string(bodyHash[:]) != event.MD5 {
		// bad hash
		return nil, errBadHash
	}
	// return body
	return &body, nil
}

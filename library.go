// BSD 2-Clause License
//
// Copyright (c) 2019 Tanner Ryan. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package ecpush

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha512"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"      // Copyright (c) 2014 Cenk Alti. All rights reserved.
	amqp "github.com/rabbitmq/amqp091-go" // Copyright (c) 2021 VMware, Inc. or its affiliates. All rights reserved.
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
	httpTimeout     = 10 * time.Second   // http fetch timeout
	httpUserAgent   = "Mozilla/5.0 (compatible; Go-http-client/1.1; +https://github.com/tannerryan/ecpush)"
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
		MinVersion: tls.VersionTLS12,
	}

	// errNoSubtopics returned when no subtopics are provided
	errNoSubtopics = errors.New("ecpush: must provide at least 1 subtopic before Connect()")
	// errBadHash returned when hash of HTTP content does not match provided
	// checksum
	errBadHash = errors.New("ecpush: provided hash does not match received content")
	// errFailedFetch returned when client fails to fetch HTTP content
	errFailedFetch = errors.New("ecpush: failed to fetch content")
)

// Client contains the ecpush consumer client.
type Client struct {
	Subtopics       *[]string // Subtopics are array of subscribed subtopics (see documentation for formatting)
	DisableEventLog bool      // DisableEventLog disables the event log (default: false)
	FetchContent    bool      // FetchContent enables HTTP content fetching (default: false)

	ctx       context.Context             // ctx is the parent context for cancellation
	event     chan *Event                 // event is client Event channel for consume function
	uid       string                      // uid is the unique client identifier
	activated atomic.Bool                 // activated indicates if consumer has been provisioned at least once
	delay     *backoff.ExponentialBackOff // delay is exponential backoff for connection recovery
	closeOnce sync.Once                   // closeOnce ensures the client is torn down exactly once

	mu   sync.Mutex       // mu guards conn and ch
	conn *amqp.Connection // conn is the active amqp.Connection
	ch   *amqp.Channel    // ch is the active amqp.Channel
}

// Event is a received payload from Environment Canada's datamart.
type Event struct {
	URL            string // URL of the product (located on Datamart)
	MD5            string // MD5 of product (if provided by the broker)
	SHA512         string // SHA512 of product (if provided by the broker)
	Route          string // Route is AMQP routing key of event
	Content        string // Content is event contents (if FetchContent is true)
	ContentFailure bool   // ContentFailure indicates if event fetching failed
}

// Connect establishes the AMQP channel for receiving products located on
// provided subtopics. It blocks until the initial connection is established
// with the remote server (or the context is cancelled). Context is passed for
// closing the client. Connect returns an error if no subtopics are provided.
func (c *Client) Connect(ctx context.Context) error {
	// ensure at least one subtopic is provided
	if c.Subtopics == nil || len(*c.Subtopics) == 0 {
		return errNoSubtopics
	}

	c.ctx = ctx
	c.event = make(chan *Event)

	// generate unique client identifier
	q1, q2 := make([]byte, 5), make([]byte, 5)
	if _, err := rand.Read(q1); err != nil {
		return fmt.Errorf("ecpush: failed to generate client identifier: %w", err)
	}
	if _, err := rand.Read(q2); err != nil {
		return fmt.Errorf("ecpush: failed to generate client identifier: %w", err)
	}
	c.uid = fmt.Sprintf("q_%s.ecpush.wx.%x.%x", user, q1, q2)

	// exponential backoff for connection recovery (retries indefinitely, capped
	// at maxRecoverDelay)
	c.delay = backoff.NewExponentialBackOff()
	c.delay.MaxInterval = maxRecoverDelay
	c.delay.Reset()

	// tear down the client once the context is cancelled
	go func() {
		<-c.ctx.Done()
		c.close()
	}()

	// run the connection/recovery loop, blocking until the first successful
	// provision or context cancellation
	ready := make(chan struct{})
	go c.run(ready)
	select {
	case <-ready:
	case <-c.ctx.Done():
	}
	return nil
}

// Consume returns the next event and an indicator if the client is no longer
// actively consuming from the remote server (i.e. the context has been
// cancelled). It blocks until an event is available or the client is closed.
func (c *Client) Consume() (*Event, bool) {
	if !c.activated.Load() {
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

// run provisions the client and re-provisions it after any disconnect or
// failure, backing off between attempts. It closes ready after the first
// successful provision and returns only on context cancellation.
func (c *Client) run(ready chan<- struct{}) {
	var signalOnce sync.Once

	for {
		if c.ctx.Err() != nil {
			return
		}

		notify, err := c.provision()
		if err != nil {
			c.log(err.Error())
			c.cleanup()
			if !c.sleep() {
				return
			}
			continue
		}

		// provisioned successfully
		c.activated.Store(true)
		c.delay.Reset()
		signalOnce.Do(func() { close(ready) })
		c.log("[ecpush] consumer activated, ready for consumption")

		// wait for a disconnect or context cancellation
		select {
		case <-c.ctx.Done():
			return
		case amqpErr := <-notify:
			if amqpErr != nil {
				c.log("[ecpush] disconnected from " + broker + ": " + amqpErr.Error())
			} else {
				c.log("[ecpush] disconnected from " + broker)
			}
			c.cleanup()
			if !c.sleep() {
				return
			}
		}
	}
}

// provision dials the broker, declares the queue/QoS/bindings, and starts the
// consumer goroutine. On success it returns a channel notified on connection
// close; otherwise it returns an error.
func (c *Client) provision() (chan *amqp.Error, error) {
	// establish connection with remote server
	uri := fmt.Sprintf("amqps://%s:%s@%s:%d/", user, pass, broker, port)
	conn, err := amqp.DialTLS(uri, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("[ecpush] failed to connect to %s: %w", broker, err)
	}
	c.log("[ecpush] connected to " + broker)

	// establish consumption channel
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("[ecpush] failed to declare channel: %w", err)
	}
	c.log("[ecpush] declared channel")

	c.mu.Lock()
	c.conn, c.ch = conn, ch
	c.mu.Unlock()

	// declare queue
	q, err := ch.QueueDeclare(
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
		return nil, fmt.Errorf("[ecpush] failed to declare queue: %w", err)
	}
	c.log("[ecpush] declared queue " + c.uid)

	// set channel quality of service
	if err = ch.Qos(
		qos,   // prefetch count
		0,     // prefetch size
		false, // global qos
	); err != nil {
		return nil, fmt.Errorf("[ecpush] failed to configure channel QoS: %w", err)
	}
	c.log("[ecpush] configured channel QoS")

	// subscribe to all subtopics
	for _, subtopic := range *c.Subtopics {
		if err = ch.QueueBind(
			q.Name,          // queue name
			prefix+subtopic, // routing key
			exchange,        // exchange
			false,           // no wait
			nil,             // arguments
		); err != nil {
			return nil, fmt.Errorf("[ecpush] failed to bind queue %s: %w", prefix+subtopic, err)
		}
		c.log("[ecpush] listening for messages on " + prefix + subtopic)
	}

	// consume from queue
	messages, err := ch.Consume(
		c.uid, // queue
		c.uid, // consumer
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("[ecpush] failed to consume messages from queue: %w", err)
	}

	c.log("[ecpush] client provisioned, activating consumer")
	go c.consume(messages)

	// notify on connection close so the run loop can recover
	return conn.NotifyClose(make(chan *amqp.Error, 1)), nil
}

// consume reads raw deliveries, transforms them into Events, and emits them on
// the event channel. It returns when the delivery channel is closed (on
// disconnect) or the context is cancelled.
func (c *Client) consume(messages <-chan amqp.Delivery) {
	for d := range messages {
		event, ok := parseDelivery(d)
		if !ok {
			// malformed payload: discard so it does not block the queue
			c.log("[ecpush] received malformed message, discarding")
			d.Ack(false)
			continue
		}

		// fetch content and update event if required
		if c.FetchContent {
			c.fetchContent(event)
		}

		// emit and acknowledge event, aborting on cancellation
		select {
		case <-c.ctx.Done():
			return
		case c.event <- event:
			d.Ack(false)
		}
	}
}

// parseDelivery transforms a raw AMQP delivery into an Event. It returns false
// if the payload is malformed.
func parseDelivery(d amqp.Delivery) (*Event, bool) {
	// payload is "<timestamp> <base-url> <relative-path>"
	parts := strings.Split(string(d.Body), " ")
	if len(parts) < 3 {
		return nil, false
	}

	event := &Event{
		URL:   parts[1] + parts[2],
		Route: d.RoutingKey,
	}

	// determine checksum algorithm (header format is "<algo>,<hash>")
	if sum, ok := d.Headers["sum"].(string); ok {
		switch {
		case strings.HasPrefix(sum, "d,"):
			event.MD5 = sum[2:] // MD5
		case strings.HasPrefix(sum, "s,"):
			event.SHA512 = sum[2:] // SHA512
		}
	}

	return event, true
}

// log internally logs events if enabled.
func (c *Client) log(data any) {
	if !c.DisableEventLog {
		log.Println(data)
	}
}

// sleep waits for the next exponential backoff interval. It returns false if
// the context is cancelled during the wait.
func (c *Client) sleep() bool {
	delay := c.delay.NextBackOff()
	c.log("[ecpush] waiting " + delay.String() + " for reconnect")
	select {
	case <-c.ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

// cleanup closes the active channel and connection, if defined. It is safe to
// call multiple times.
func (c *Client) cleanup() {
	c.mu.Lock()
	ch, conn := c.ch, c.conn
	c.ch, c.conn = nil, nil
	c.mu.Unlock()

	if ch != nil {
		ch.Close()
	}
	if conn != nil {
		conn.Close()
	}
}

// close terminates the provisioned AMQP channel and connection to the remote
// server. It is invoked once when the context is cancelled.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.log("[ecpush] received context cancellation, terminating consumer")
		c.activated.Store(false)

		c.mu.Lock()
		ch, conn := c.ch, c.conn
		c.ch, c.conn = nil, nil
		c.mu.Unlock()

		// cancel and close channel
		if ch != nil {
			ch.Cancel(c.uid, true)
			ch.Close()
		}
		// close connection
		if conn != nil {
			conn.Close()
		}
	})
}

// fetchContent accepts an Event and attempts to populate the Event with content
// located at Event URL. If the content can not be fetched after multiple
// attempts, the ContentFailure flag is set to true in the Event.
func (c *Client) fetchContent(event *Event) {
	content, err := fetchEvent(c.ctx, event)
	if err != nil {
		c.log("[ecpush] failed to fetch event content: " + err.Error())
		event.ContentFailure = true
		return
	}
	// populate event content
	event.Content = string(content)
}

// fetchEvent attempts to fetch the HTTP contents of an Event, retrying up to
// contentAttempts times. It returns the byte content of the event or the last
// error encountered if all attempts fail.
func fetchEvent(ctx context.Context, event *Event) ([]byte, error) {
	var lastErr error
	for range contentAttempts {
		body, err := fetchOnce(ctx, event)
		if err == nil {
			return body, nil
		}
		lastErr = err
		// abort early if the context has been cancelled
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = errFailedFetch
	}
	return nil, lastErr
}

// fetchOnce performs a single HTTP fetch of an Event and validates the content
// checksum, if one was provided.
func fetchOnce(ctx context.Context, event *Event) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, event.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", httpUserAgent)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: unexpected status %s", errFailedFetch, resp.Status)
	}

	// read body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// validate checksum
	if event.MD5 != "" {
		if fmt.Sprintf("%x", md5.Sum(body)) != event.MD5 {
			return nil, errBadHash
		}
	}
	if event.SHA512 != "" {
		if fmt.Sprintf("%x", sha512.Sum512(body)) != event.SHA512 {
			return nil, errBadHash
		}
	}

	return body, nil
}

// Copyright (c) 2018 Tanner Ryan. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
Package ecpush is a library for subscribing to real-time
meteorological data feeds from Environment Canada.

Goals

The main goal of ecpush is to provide a simple and lightweight
client that can be used for receiving real-time data events
directly from Environment Canada's meteorological product
feed.

The client can directly fetch the published products, or it can
just provide a notification channel containing the product
location (HTTP URL to Environment Canada's Datamart). The
client has also been designed to automatically recover from
any connection or channel interruptions.

Usage

The interface is very minimal. To create a new client, simply
create a Client struct. The only required field in the struct
is the Subtopics array. Default values for the other fields
are listed in the struct definition. An example configuration
is shown below (subscribing to text bulletins, citypage XML and
CAP alert files).

Please see https://github.com/MetPX/sarracenia/blob/master/doc/sr_subscribe.1.rst#subtopic-amqp-pattern-subtopic-need-to-be-set
for formatting subtopics.

	client := &ecpush.Client{
		Subtopics: &[]string{
			"alerts.cap.#",
			"bulletins.alphanumeric.#",
			"citypage_weather.xml.#",
		},
		DisableEventLog: false,
		FetchContent:    false,
	}

When calling `Connect()` on the newly created client, two
channels will be returned. To receive the Events, create
a goroutine to range over the Event channel. The done channel
may be used to block the goroutine.

	msg, done := client.Connect()
	go func() {
		for event := range msg {
			log.Printf("[x] %s\n", event.URL)
		}
	}()
	<-done

To close the ecpush Client, simply call Close() on the client. This will
close all active channels and connections to the messaging broker. It
will also signal the done channel which will close the holding goroutine
previously created above.

	client.Close()

Examples

A fully functioning client can be found in the example directory.

Acknowledgements

I would like to thank Sean Treadway for his Go RabbitMQ client library.
I would also like to thank Environment Canada and the awesome people at
Shared Services Canada for their developments and "openness" of MetPX
and sarracenia.

License

Copyright (c) 2018 Tanner Ryan. All rights reserved. Use of this source
code is governed by a MIT license that can be found in the LICENSE
file.

Sean Treadway's Go RabbitMQ client library is under a BSD 2-clause
license. Once again, all rights reserved.

*/
package ecpush

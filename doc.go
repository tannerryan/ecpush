// Copyright (c) 2019 Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package ecpush is a library for subscribing to real-time meteorological data
feeds from Environment Canada.

Goals

The main goal of ecpush is to provide a simple and lightweight client that can
be used for receiving real-time data events directly from Environment Canada's
meteorological product feed.

The client can directly fetch the published products, or it can just provide a
notification channel containing the product location (HTTP URL to Environment
Canada's Datamart). The client has also been designed to automatically recover
from any connection or channel interruptions.

Usage

To create a new client, create a Client struct. The only required field is the
Subtopics array. Default values for other fields are listed in the struct
definition. An example configuration is shown below (subscribing text bulletins,
citypage XML and CAP alert files).

Please see
https://github.com/MetPX/sarracenia/blob/master/doc/sr_subscribe.1.rst#subtopic-amqp-pattern-subtopic-need-to-be-set
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

Calling Connect(ctx) will return an error if no subtopics are provided. The
function will block until the initial connection with the remote server is
established. When the client is provisioned, an internal Goroutine is created to
consume the feed.

    // create context for closing client
    ctx := context.Background()
    ctx, cancel := context.WithCancel(ctx)

    err := client.Connect(ctx)
    if err != nil {
        panic(err)
    }

To consume the events, call Consume() on the client. This returns an Event and
an indicator if the client is still actively consuming from the remote server.

    for {
        event, closed := client.Consume()
        if closed {
            // not actively consuming
            return
        }
        log.Println(event)
    }

To close the client, call the cancel function on the context provided to the
client. This will gracefully close the active channels and connection to the
remote server.

    close()

Examples

A fully functioning client can be found in the example directory.

Acknowledgements

I would like to thank Sean Treadway for his Go RabbitMQ client client. I would
also like to thank Environment Canada and the awesome people at Shared Services
Canada for their developments and "openness" of MetPX and sarracenia.

License

Copyright (c) 2019 Tanner Ryan. All rights reserved. Use of this source code is
governed by a BSD-style license that can be found in the LICENSE file.

Sean Treadway's Go RabbitMQ client client is under a BSD 2-clause license. Cenk
Alti's Go exponential backoff package is under an MIT license. Once again, all
rights reserved.

*/
package ecpush

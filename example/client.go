// Copyright (c) 2019 Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thetannerryan/ecpush"
)

func main() {
	// create context for closing client
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	client := &ecpush.Client{
		Subtopics: &[]string{
			"alerts.cap.#",
			"bulletins.alphanumeric.#",
			"citypage_weather.xml.#",
		}, // array of subscribed subtopics (see documentation for formatting)
		DisableEventLog: false, // disable event log (default value)
		FetchContent:    false, // enable HTTP content fetching (default value)
	}

	// connect to client
	err := client.Connect(ctx)
	if err != nil {
		panic(err)
	}

	go func() {
		// kill client after 15 seconds
		time.Sleep(15 * time.Second)
		fmt.Println("closing client after 15 seconds")
		cancel()
	}()

	for {
		event, closed := client.Consume()
		if closed {
			// not actively consuming
			return
		}
		log.Println("[x]", event)
	}
}

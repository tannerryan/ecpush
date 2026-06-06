// Copyright (c) 2019 Tanner Ryan. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/tannerryan/ecpush"
)

func main() {
	// cancel the client on interrupt or termination
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := &ecpush.Client{
		Subtopics: &[]string{
			"*.WXO-DD.citypage_weather.ON.#",
			"*.WXO-DD.bulletins.alphanumeric.#",
		}, // array of subscribed subtopics (see documentation for formatting)
		DisableEventLog: false, // disable event log (default value)
		FetchContent:    false, // enable HTTP content fetching (default value)
	}

	// connect to client
	if err := client.Connect(ctx); err != nil {
		panic(err)
	}

	for {
		event, closed := client.Consume()
		if closed {
			// not actively consuming
			return
		}
		log.Println("[x]", event)
	}
}

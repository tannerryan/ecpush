// Copyright (c) 2019 Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"log"

	"github.com/tannerryan/ecpush"
)

func main() {
	// create context for closing client
	ctx := context.Background()

	client := &ecpush.Client{
		Subtopics: &[]string{
			"*.WXO-DD.citypage_weather.ON.#",
			"*.WXO-DD.bulletins.alphanumeric.#",
		}, // array of subscribed subtopics (see documentation for formatting)
		DisableEventLog: false, // disable event log (default value)
		FetchContent:    false, // enable HTTP content fetching (default value)
	}

	// connect to client
	err := client.Connect(ctx)
	if err != nil {
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

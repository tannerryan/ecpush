// Copyright (c) 2018 Tanner Ryan. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"log"

	"github.com/thetannerryan/ecpush"
)

func main() {
	client := &ecpush.Client{
		Subtopics: &[]string{
			"alerts.cap.#",
			"bulletins.alphanumeric.#",
			"citypage_weather.xml.#",
		}, // array of subscribed subtopics (see documentation for formatting)
		DisableEventLog: false, // disable event log (default value)
		FetchContent:    false, // enable HTTP content fetching (default value)
	}

	msg, done := client.Connect()
	go func() {
		for event := range msg {
			log.Printf("[x] %s\n", event.URL)
		}
	}()
	<-done
}

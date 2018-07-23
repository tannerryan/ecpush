// Copyright (c) 2018, Tanner Ryan. All rights reserved.
// Use of this source code is governed by a BSD 2-clause
// license that can be found in the LICENSE file.

package main

import (
	"log"

	"github.com/thetannerryan/ecpush"
)

func main() {
	client := ecpush.Client{
		Subtopics: []string{
			"bulletins.alphanumeric.#",
			"citypage_weather.xml.#",
		}, // array of subscribed subtopics (see above for formatting)
		DisableRecovery:     false, // (default value) disable connection and fault recovery
		DisableEventLog:     false, // (default value) disable event log
		ReconnectDelay:      30,    // (default value) amqp reconnect delay
		NotifyOnly:          false, // (default value) disable HTTP content fetching
		DisableContentRetry: false, // (default value) disable multiple HTTP fetches in event of request failure
		ContentAttempts:     3,     // (default value) number of HTTP fetch attempts
	}

	if msg, done := client.Connect(); done != nil {
		go func() {
			for event := range msg {
				log.Println(event.URL)
				// Available Event Fields
				//	- event.URL :: HTTP URL of event
				//	- event.Md5 :: MD5 checksum of event content
				//	- event.Content :: verified event content (if NotifyOnly is disabled)
				//	- event.ContentFailure :: indicator if content fetch failed (Content will be empty string)
			}
		}()
		<-done
	}
}

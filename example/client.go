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
		Subtopics: []string{"bulletins.alphanumeric.#", "citypage_weather.xml.#"},
	}

	if msg, done := client.Connect(); done != nil {
		go func() {
			for event := range msg {
				log.Printf("%s; %s\n", event.URL, event.Md5)
			}
		}()
		<-done
	}
}

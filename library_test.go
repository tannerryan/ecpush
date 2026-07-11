// Copyright (c) 2019 Tanner Ryan. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

package ecpush

import (
	"context"
	"crypto/md5"
	"crypto/sha512"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestParseDelivery(t *testing.T) {
	md5Hash := fmt.Sprintf("%x", md5.Sum([]byte("body")))
	shaHash := fmt.Sprintf("%x", sha512.Sum512([]byte("body")))

	tests := []struct {
		name       string
		body       string
		sum        any
		ok         bool
		wantURL    string
		wantMD5    string
		wantSHA512 string
	}{
		{
			name:    "md5 header",
			body:    "20200101 https://dd.weather.gc.ca/ path/file.xml",
			sum:     "d," + md5Hash,
			ok:      true,
			wantURL: "https://dd.weather.gc.ca/path/file.xml",
			wantMD5: md5Hash,
		},
		{
			name:       "sha512 header",
			body:       "20200101 https://dd.weather.gc.ca/ path/file.xml",
			sum:        "s," + shaHash,
			ok:         true,
			wantURL:    "https://dd.weather.gc.ca/path/file.xml",
			wantSHA512: shaHash,
		},
		{
			name:    "missing sum header",
			body:    "20200101 https://dd.weather.gc.ca/ path/file.xml",
			sum:     nil,
			ok:      true,
			wantURL: "https://dd.weather.gc.ca/path/file.xml",
		},
		{
			name:    "non-string sum header",
			body:    "20200101 https://dd.weather.gc.ca/ path/file.xml",
			sum:     42,
			ok:      true,
			wantURL: "https://dd.weather.gc.ca/path/file.xml",
		},
		{
			name:    "extra whitespace",
			body:    "20200101   https://dd.weather.gc.ca/   path/file.xml",
			sum:     nil,
			ok:      true,
			wantURL: "https://dd.weather.gc.ca/path/file.xml",
		},
		{
			name: "too few fields",
			body: "20200101 https://dd.weather.gc.ca/",
			ok:   false,
		},
		{
			name: "empty body",
			body: "",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := amqp.Table{}
			if tt.sum != nil {
				headers["sum"] = tt.sum
			}
			event, ok := parseDelivery(amqp.Delivery{Body: []byte(tt.body), Headers: headers})
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if event.URL != tt.wantURL {
				t.Errorf("URL = %q, want %q", event.URL, tt.wantURL)
			}
			if event.MD5 != tt.wantMD5 {
				t.Errorf("MD5 = %q, want %q", event.MD5, tt.wantMD5)
			}
			if event.SHA512 != tt.wantSHA512 {
				t.Errorf("SHA512 = %q, want %q", event.SHA512, tt.wantSHA512)
			}
		})
	}
}

func TestFetchEvent(t *testing.T) {
	const payload = "hello world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/notfound":
			w.WriteHeader(http.StatusNotFound)
		default:
			fmt.Fprint(w, payload)
		}
	}))
	defer srv.Close()

	t.Run("valid md5", func(t *testing.T) {
		event := &Event{URL: srv.URL + "/ok", MD5: fmt.Sprintf("%x", md5.Sum([]byte(payload)))}
		body, err := fetchEvent(context.Background(), event)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(body) != payload {
			t.Errorf("body = %q, want %q", body, payload)
		}
	})

	t.Run("bad checksum", func(t *testing.T) {
		event := &Event{URL: srv.URL + "/ok", SHA512: "deadbeef"}
		if _, err := fetchEvent(context.Background(), event); !errors.Is(err, errBadHash) {
			t.Fatalf("err = %v, want errBadHash", err)
		}
	})

	t.Run("non-200 status", func(t *testing.T) {
		event := &Event{URL: srv.URL + "/notfound"}
		if _, err := fetchEvent(context.Background(), event); !errors.Is(err, errFailedFetch) {
			t.Fatalf("err = %v, want errFailedFetch", err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		event := &Event{URL: srv.URL + "/ok"}
		if _, err := fetchEvent(ctx, event); err == nil {
			t.Fatal("expected error for cancelled context")
		}
	})
}

func TestConnectNoSubtopics(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		c := &Client{}
		if err := c.Connect(nil); !errors.Is(err, errNilContext) {
			t.Fatalf("err = %v, want errNilContext", err)
		}
	})

	t.Run("nil subtopics", func(t *testing.T) {
		c := &Client{}
		if err := c.Connect(context.Background()); !errors.Is(err, errNoSubtopics) {
			t.Fatalf("err = %v, want errNoSubtopics", err)
		}
	})

	t.Run("empty subtopics", func(t *testing.T) {
		c := &Client{Subtopics: &[]string{}}
		if err := c.Connect(context.Background()); !errors.Is(err, errNoSubtopics) {
			t.Fatalf("err = %v, want errNoSubtopics", err)
		}
	})
}

func TestConnectCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	subtopics := []string{"*.WXO-DD.citypage_weather.ON.#"}
	c := &Client{Subtopics: &subtopics}
	if err := c.Connect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestConsumeBeforeConnect(t *testing.T) {
	c := &Client{}
	if event, closed := c.Consume(); event != nil || !closed {
		t.Fatalf("Consume() = (%v, %v), want (nil, true)", event, closed)
	}
}

// Copyright (c) 2019 Tanner Ryan. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

package ecpush

import (
	"context"
	"crypto/md5"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	useHTTPTransport(t, func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		status := http.StatusOK
		switch r.URL.Path {
		case "/notfound":
			status = http.StatusNotFound
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
	})
	const baseURL = "https://dd.weather.gc.ca"

	t.Run("valid md5", func(t *testing.T) {
		event := &Event{URL: baseURL + "/ok", MD5: fmt.Sprintf("%x", md5.Sum([]byte(payload)))}
		body, err := fetchEvent(context.Background(), event)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(body) != payload {
			t.Errorf("body = %q, want %q", body, payload)
		}
	})

	t.Run("bad checksum", func(t *testing.T) {
		event := &Event{URL: baseURL + "/ok", SHA512: "deadbeef"}
		if _, err := fetchEvent(context.Background(), event); !errors.Is(err, errBadHash) {
			t.Fatalf("err = %v, want errBadHash", err)
		}
	})

	t.Run("non-200 status", func(t *testing.T) {
		event := &Event{URL: baseURL + "/notfound"}
		if _, err := fetchEvent(context.Background(), event); !errors.Is(err, errFailedFetch) {
			t.Fatalf("err = %v, want errFailedFetch", err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		event := &Event{URL: baseURL + "/ok"}
		if _, err := fetchEvent(ctx, event); err == nil {
			t.Fatal("expected error for cancelled context")
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Keep the real redirect policy while replacing all network access.
func useHTTPTransport(t *testing.T, transport roundTripFunc) {
	t.Helper()
	old := httpClient
	client := *old
	client.Transport = transport
	httpClient = &client
	t.Cleanup(func() { httpClient = old })
}

func TestFetchDestinations(t *testing.T) {
	for _, tt := range []struct {
		url     string
		allowed bool
	}{
		{"https://dd.weather.gc.ca/product", true},
		{"https://dd.weather.gc.ca:443/product", true},
		{"https://weather.gc.ca/product", true},
		{"https://dd1.weather.gc.ca/product", true},
		{"https://dd2.weather.gc.ca/product", true},
		{"https://another.weather.gc.ca/product", true},
		{"https://DD3.WEATHER.GC.CA/product", true},
		{"https://meteo.gc.ca/product", true},
		{"https://dd.meteo.gc.ca/product", true},
		{"https://DD3.METEO.GC.CA/product", true},
		{"http://dd.weather.gc.ca/product", true},
		{"http://dd.weather.gc.ca:80/product", true},
		{"http://dd4.weather.gc.ca:80/product", true},
		{"http://dd.meteo.gc.ca/product", true},
		{"http://dd4.meteo.gc.ca:80/product", true},
		{"https://example.com/product", false},
		{"https://dd.weather.gc.ca.example.com/product", false},
		{"https://fakeweather.gc.ca/product", false},
		{"https://fakemeteo.gc.ca/product", false},
		{"https://dd.meteo.gc.ca.example.com/product", false},
		{"http://127.0.0.1/product", false},
		{"https://dd.weather.gc.ca:8443/product", false},
		{"https://dd.meteo.gc.ca:8443/product", false},
		{"https://user:pass@dd.weather.gc.ca/product", false},
		{"file:///product", false},
	} {
		t.Run(tt.url, func(t *testing.T) {
			calls := 0
			useHTTPTransport(t, func(req *http.Request) (*http.Response, error) {
				calls++
				if req.URL.Scheme != "https" {
					t.Fatalf("unexpected destination: %s", req.URL)
				}
				if req.Host != req.URL.Host {
					t.Fatalf("Host = %q, want %q", req.Host, req.URL.Host)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})
			_, err := fetchEvent(context.Background(), &Event{URL: tt.url})
			if tt.allowed {
				if err != nil || calls != 1 {
					t.Fatalf("err = %v, requests = %d", err, calls)
				}
			} else if !errors.Is(err, errContentURL) || calls != 0 {
				t.Fatalf("err = %v, requests = %d; want rejected without a request", err, calls)
			}
		})
	}
}

func TestFetchRedirects(t *testing.T) {
	for _, tt := range []struct {
		target  string
		allowed bool
	}{
		{"/next", true},
		{"https://another.weather.gc.ca/next", true},
		{"https://meteo.gc.ca/next", true},
		{"https://dd.meteo.gc.ca/next", true},
		{"https://example.com/next", false},
		{"https://fakemeteo.gc.ca/next", false},
		{"https://dd.meteo.gc.ca.example.com/next", false},
		{"http://dd.weather.gc.ca/next", false},
		{"http://dd.meteo.gc.ca/next", false},
		{"https://dd.weather.gc.ca:8443/next", false},
		{"https://user:pass@dd.weather.gc.ca/next", false},
	} {
		t.Run(tt.target, func(t *testing.T) {
			calls := 0
			useHTTPTransport(t, func(req *http.Request) (*http.Response, error) {
				calls++
				if req.URL.Path == "/start" {
					return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {tt.target}}, Body: http.NoBody}, nil
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})
			_, err := fetchEvent(context.Background(), &Event{URL: "https://dd.weather.gc.ca/start"})
			if tt.allowed {
				if err != nil || calls != 2 {
					t.Fatalf("err = %v, requests = %d", err, calls)
				}
			} else if !errors.Is(err, errContentURL) || calls != 1 {
				t.Fatalf("err = %v, requests = %d; want redirect rejected without retry", err, calls)
			}
		})
	}
}

type sizedBody struct {
	remaining int64
	read      int64
	closed    bool
}

func (b *sizedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), b.remaining))
	clear(p[:n])
	b.remaining -= int64(n)
	b.read += int64(n)
	return n, nil
}

func (b *sizedBody) Close() error { b.closed = true; return nil }

func TestFetchContentLimit(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		size, length, wantRead int64
		tooLarge               bool
	}{
		{"declared too large", maxContentSize + 1, maxContentSize + 1, 0, true},
		{"unknown length too large", maxContentSize + 1024, -1, maxContentSize + 1, true},
		{"exact limit", maxContentSize, -1, maxContentSize, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &sizedBody{remaining: tt.size}
			calls := 0
			useHTTPTransport(t, func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: tt.length}, nil
			})
			content, err := fetchEvent(context.Background(), &Event{URL: "https://dd.weather.gc.ca/product"})
			if tt.tooLarge {
				if !errors.Is(err, errContentTooLarge) || content != nil {
					t.Fatalf("got %d bytes, err = %v; want oversized response rejected", len(content), err)
				}
			} else if err != nil || len(content) != maxContentSize {
				t.Fatalf("got %d bytes, err = %v", len(content), err)
			}
			if calls != 1 || body.read != tt.wantRead || !body.closed {
				t.Fatalf("requests = %d, bytes read = %d, closed = %v", calls, body.read, body.closed)
			}
		})
	}
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
